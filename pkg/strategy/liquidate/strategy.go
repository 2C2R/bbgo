package liquidate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

const ID = "liquidate"

var log = logrus.WithField("strategy", ID)

func init() {
	bbgo.RegisterStrategy(ID, &Strategy{})
}

type Strategy struct {
	Symbol                 string         `json:"symbol"`
	Side                   string         `json:"side"`
	UpdateInterval         types.Duration `json:"updateInterval"`
	OffsetTick             int            `json:"offsetTick"`
	DryRun                 bool           `json:"dryRun"`
	MaxPriceDropPercentage float64        `json:"maxPriceDropPercentage"` // e.g., 0.02 for 2%

	Market   types.Market
	Position *types.Position

	session                  *bbgo.ExchangeSession
	orderExecutor            *bbgo.GeneralOrderExecutor
	orderBook                *types.StreamOrderBook
	lastUpdateTime           time.Time
	lastCalculatedSellPrice  fixedpoint.Value // Stores the last sell price that passed the drop check
	maxPriceDropF            fixedpoint.Value // Parsed MaxPriceDropPercentage as fixedpoint
	lastKnownRelevantBalance fixedpoint.Value
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	session.Subscribe(types.BookChannel, s.Symbol, types.SubscribeOptions{})
}

func (s *Strategy) placeOrder(ctx context.Context) error {
	// Check trading direction
	side := types.SideTypeBuy
	if s.Side == "SELL" {
		side = types.SideTypeSell
	}

	// Get price from orderbook instead of ticker
	ob := s.orderBook.Copy()
	// Determine which balance and price to use based on trading direction
	var price fixedpoint.Value

	// Calculate price first
	if side == types.SideTypeBuy {
		bestBid, ok := ob.BestBid()
		if !ok {
			return fmt.Errorf("best bid not found")
		}
		bestAsk, ok := ob.BestAsk()
		if !ok {
			return fmt.Errorf("best ask not found")
		}

		// Revised buy-side logic:
		// Try to place a buy order at a price slightly higher than the current best bid,
		// but still lower than the best ask, to become the new best bid.
		if bestBid.Price.Add(s.Market.TickSize).Compare(bestAsk.Price) < 0 {
			// If best_bid + 1 tick is still less than best_ask, we can place our order at best_bid + 1 tick.
			price = bestBid.Price.Add(s.Market.TickSize)
			log.Infof("best bid %s + 1 tick < best ask %s, submit new best bid price: %s", bestBid.Price.String(), bestAsk.Price.String(), price.String())
		} else if bestBid.Price.Add(s.Market.TickSize).Compare(bestAsk.Price) == 0 {
			// If best_bid + 1 tick is equal to best_ask, we place our order at current best_bid to avoid crossing the spread.
			// Or, if the spread is already 1 tick (best_ask - best_bid == tick_size), placing at best_bid is appropriate.
			price = bestBid.Price
			log.Infof("best bid %s + 1 tick >= best ask %s (or spread is 1 tick), submit current best bid price: %s", bestBid.Price.String(), bestAsk.Price.String(), price.String())
		} else {
			// This case implies bestBid.Price.Add(s.Market.TickSize).Compare(bestAsk.Price) > 0
			// which means best_bid + 1 tick > best_ask. This should not happen in a typical healthy order book
			// unless the spread is zero or negative (crossed book).
			// Or, it could mean the market is very thin and placing a more aggressive order is not desired by this logic.
			// We can choose to place at bestBid if it's not worse than bestAsk.
			if bestBid.Price.Compare(bestAsk.Price) < 0 {
				price = bestBid.Price
				log.Infof("best bid + 1 tick > best ask. Spread is very small or crossed. Submitting at current best bid price: %s", price.String())
			} else {
				return fmt.Errorf("malformed orderbook or unable to place buy order without crossing spread: best bid %s, best ask %s", bestBid.Price.String(), bestAsk.Price.String())
			}
		}

		if s.OffsetTick > 0 {
			price = price.Sub(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}
	} else {
		bestAsk, ok := ob.BestAsk()
		if !ok {
			return fmt.Errorf("best ask not found")
		}
		bestBid, ok := ob.BestBid()
		if !ok {
			return fmt.Errorf("best bid not found")
		}
		log.Infof("best bid: %s, best ask: %s", bestBid.Price.String(), bestAsk.Price.String())
		// If best ask sub 1 tick size is greater than best bid, use best ask sub 1 tick size to make sure we can sell first
		if bestAsk.Price.Sub(s.Market.TickSize) > bestBid.Price {
			price = bestAsk.Price.Sub(s.Market.TickSize)
			log.Infof("best ask %s - 1 tick > best bid %s, submit new best ask price: %s", bestAsk.Price.String(), bestBid.Price.String(), price.String())
		} else if bestAsk.Price.Sub(s.Market.TickSize).Compare(bestBid.Price) == 0 {
			price = bestAsk.Price
			log.Infof("best ask %s - 1 tick == best bid %s, submit current best ask price: %s", bestAsk.Price.String(), bestBid.Price.String(), price.String())
		} else {
			return fmt.Errorf("malformed orderbook, best ask %s - 1 tick < best bid %s", bestAsk.Price.String(), bestBid.Price.String())
		}
		if s.OffsetTick > 0 {
			price = price.Add(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}

		// Price Drop Check for SELL orders
		if !s.maxPriceDropF.IsZero() { // Check if feature is enabled
			if !s.lastCalculatedSellPrice.IsZero() { // If we have a previous price to compare against
				allowedDrop := s.lastCalculatedSellPrice.Mul(s.maxPriceDropF)
				minimumAcceptablePrice := s.lastCalculatedSellPrice.Sub(allowedDrop)

				if price.Compare(minimumAcceptablePrice) < 0 {
					log.Warnf("[%s] SELL price %s is %.2f%% below last known good price %s (min acceptable %s). Skipping order this cycle.",
						s.Symbol, price.String(), s.MaxPriceDropPercentage*100, s.lastCalculatedSellPrice.String(), minimumAcceptablePrice.String())
					return nil // Skip placing order this cycle
				}
				// Price is acceptable, update lastCalculatedSellPrice with the current price
				s.lastCalculatedSellPrice = price
				log.Debugf("[%s] SELL price %s passed drop check. Updated lastCalculatedSellPrice.", s.Symbol, price.String())
			} else {
				// This is the first sell price calculation with the check enabled.
				// So, use the current price as the baseline.
				s.lastCalculatedSellPrice = price
				log.Infof("[%s] Initialized lastCalculatedSellPrice for SELL to %s (price drop check enabled).", s.Symbol, price.String())
			}
		}
	}

	// Check if we already have active orders at the same price
	activeOrders := s.orderExecutor.ActiveMakerOrders()
	for _, order := range activeOrders.Orders() {
		if order.Price.Compare(price) == 0 {
			log.Infof("existing order found at price %s, skipping", price.String())
			return nil
		}
	}

	// Cancel existing orders if any
	if activeOrders.NumOfOrders() > 0 {
		if err := s.orderExecutor.GracefulCancel(ctx); err != nil {
			log.WithError(err).Errorf("cannot cancel orders")
			return err
		}
		log.Infof("cancelled active orders")
	}

	// Calculate quantity using latest balance
	balances := s.session.GetAccount().Balances()
	var quantity fixedpoint.Value

	if side == types.SideTypeBuy {
		quoteBalance, ok := balances[s.Market.QuoteCurrency]
		if !ok {
			return fmt.Errorf("quote balance %s not found", s.Market.QuoteCurrency)
		}
		quantity = quoteBalance.Available.Div(price)
	} else {
		baseBalance, ok := balances[s.Market.BaseCurrency]
		if !ok {
			return fmt.Errorf("base balance %s not found", s.Market.BaseCurrency)
		}
		quantity = baseBalance.Available
	}

	// Check min quantity
	if quantity.Compare(s.Market.MinQuantity) < 0 {
		log.Infof("%s is less than %s market min quantity %s, skipping", quantity.String(), s.Symbol, s.Market.MinQuantity.String())
		return nil
	}

	if s.DryRun {
		log.Infof("[DryRun] would submit order: Symbol=%s Side=%s Type=%s Price=%s Quantity=%s TimeInForce=%s",
			s.Symbol, side, types.OrderTypeLimitMaker, price.String(), quantity.String(), types.TimeInForceGTC)
		return nil
	}

	createdOrders, err := s.orderExecutor.SubmitOrders(ctx, types.SubmitOrder{
		Symbol:      s.Symbol,
		Side:        side,
		Type:        types.OrderTypeLimitMaker,
		Price:       price,
		Quantity:    quantity,
		TimeInForce: types.TimeInForceGTC,
	})

	if err != nil {
		return err
	}

	log.Infof("order submitted: %+v", createdOrders)
	return nil
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	s.session = session
	if s.Position == nil {
		s.Position = types.NewPosition(s.Symbol, s.Market.BaseCurrency, s.Market.QuoteCurrency)
	}
	s.orderExecutor = bbgo.NewGeneralOrderExecutor(session, s.Symbol, ID, "", s.Position)
	s.orderBook = types.NewStreamBook(s.Symbol, session.Exchange.Name())
	s.orderBook.BindStream(session.MarketDataStream)

	// Get market info
	market, ok := session.Market(s.Symbol)
	if !ok {
		return fmt.Errorf("market %s not found", s.Symbol)
	}
	s.Market = market

	// Initialize lastKnownRelevantBalance
	s.initializeLastKnownRelevantBalance()

	// Initialize MaxPriceDropPercentage fixedpoint value
	if s.MaxPriceDropPercentage > 0 && s.MaxPriceDropPercentage < 1.0 { // Basic validation e.g. 0.01 for 1%, 0.99 for 99%
		s.maxPriceDropF = fixedpoint.NewFromFloat(s.MaxPriceDropPercentage)
		log.Infof("[%s] Price drop check enabled for SELL side with max drop of %.2f%%", s.Symbol, s.MaxPriceDropPercentage*100)
	} else if s.MaxPriceDropPercentage != 0 { // if it's 0, it's disabled (which is fine). Otherwise, it's an invalid value.
		log.Warnf("[%s] MaxPriceDropPercentage %.2f is invalid. It should be between 0.0 (exclusive, for enabling) and 1.0 (exclusive). Disabling price drop check.", s.Symbol, s.MaxPriceDropPercentage)
		s.maxPriceDropF = fixedpoint.Zero // Ensure it's zero, effectively disabling it
	}

	// Listen to orderbook updates
	s.orderBook.OnUpdate(func(book types.SliceOrderBook) {
		if time.Since(s.lastUpdateTime) < s.UpdateInterval.Duration() {
			return
		}
		s.lastUpdateTime = time.Now()
		if err := s.placeOrder(ctx); err != nil {
			log.WithError(err).Error("cannot place order")
		}
	})

	// Subscribe to balance updates
	if !s.session.PublicOnly {
		s.session.UserDataStream.OnBalanceUpdate(func(balances types.BalanceMap) {
			s.handleBalanceUpdate(ctx, balances)
		})
		// Also handle initial balance snapshot if needed, or rely on first balance update.
		// For immediate action on startup after deposit, checking current balance and placing order
		// if conditions met could be done here or after initial lastKnownRelevantBalance setup.
		// Let's ensure placeOrder is attempted once if conditions are met after initialization.
		log.Infof("[%s] Performing initial check for order placement potential after setup.", s.Symbol)
		if err := s.placeOrder(ctx); err != nil {
			log.WithError(err).Errorf("[%s] Error during initial order placement check: %v", s.Symbol, err)
		} else {
			// if placeOrder was successful (or skipped appropriately), update time
			s.lastUpdateTime = time.Now()
		}
	}

	// the shutdown handler, you can cancel all orders
	bbgo.OnShutdown(ctx, func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		_ = s.orderExecutor.GracefulCancel(ctx)
		bbgo.Sync(ctx, s)
	})

	return nil
}

func (s *Strategy) initializeLastKnownRelevantBalance() {
	balances := s.session.GetAccount().Balances()
	relevantCurrency := s.Market.QuoteCurrency // Default for BUY side
	if s.Side == "SELL" {
		relevantCurrency = s.Market.BaseCurrency
	}

	if balance, ok := balances[relevantCurrency]; ok {
		s.lastKnownRelevantBalance = balance.Available
		log.Infof("[%s] Initialized lastKnownRelevantBalance for %s: %s", s.Symbol, relevantCurrency, s.lastKnownRelevantBalance.String())
	} else {
		s.lastKnownRelevantBalance = fixedpoint.Zero
		log.Warnf("[%s] Could not find initial balance for %s. lastKnownRelevantBalance set to 0.", s.Symbol, relevantCurrency)
	}
}

func (s *Strategy) handleBalanceUpdate(ctx context.Context, updatedBalances types.BalanceMap) {
	relevantCurrency := s.Market.QuoteCurrency // Default for BUY side
	if s.Side == "SELL" {
		relevantCurrency = s.Market.BaseCurrency
	}

	newBalance, ok := updatedBalances[relevantCurrency]
	if !ok {
		// log.Debugf("[%s] Balance update received, but relevant currency %s not found in the update.", s.Symbol, relevantCurrency)
		// We should still update our knowledge of other balances if they affect overall account state.
		// However, for triggering based on a specific currency deposit, we only care about that one.
		// Let's re-fetch the full balances to update our lastKnownRelevantBalance accurately.
		currentBalances := s.session.GetAccount().Balances()
		if currentRelBalance, ok := currentBalances[relevantCurrency]; ok {
			s.lastKnownRelevantBalance = currentRelBalance.Available
		}
		return
	}

	log.Infof("[%s] Balance update for %s: Old Known: %s, New Available in Update: %s", s.Symbol, relevantCurrency, s.lastKnownRelevantBalance.String(), newBalance.Available.String())

	// Check if the available balance for the relevant currency has increased
	if newBalance.Available.Compare(s.lastKnownRelevantBalance) > 0 {
		log.Infof("[%s] Detected deposit for %s. New available balance: %s (was %s). Attempting to place order.",
			s.Symbol, relevantCurrency, newBalance.Available.String(), s.lastKnownRelevantBalance.String())

		// Update lastKnownRelevantBalance before trying to place order
		s.lastKnownRelevantBalance = newBalance.Available

		if err := s.placeOrder(ctx); err != nil {
			log.WithError(err).Errorf("[%s] Error placing order after balance update: %v", s.Symbol, err)
		} else {
			log.Infof("[%s] Order placement attempt after balance update completed.", s.Symbol)
			s.lastUpdateTime = time.Now() // Reset interval timer as we've just acted
		}
	} else if newBalance.Available.Compare(s.lastKnownRelevantBalance) < 0 {
		log.Infof("[%s] Balance for %s decreased or stayed the same. New available: %s (was %s). No deposit detected.",
			s.Symbol, relevantCurrency, newBalance.Available.String(), s.lastKnownRelevantBalance.String())
		// Update lastKnownRelevantBalance even if it decreased or stayed same
		s.lastKnownRelevantBalance = newBalance.Available
	} else {
		// Balance is the same, no action needed, but update lastKnownRelevantBalance just in case (should be redundant if logic is correct)
		s.lastKnownRelevantBalance = newBalance.Available
	}
}
