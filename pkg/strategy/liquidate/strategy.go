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
			// If best_bid + 1 tick is equal to best_ask (spread is 1 tick),
			// we place our order at current best_bid to avoid crossing the spread and act as a maker.
			price = bestBid.Price
			log.Infof("best bid %s + 1 tick == best ask %s (spread is 1 tick), submit current best bid price: %s", bestBid.Price.String(), bestAsk.Price.String(), price.String())
		} else {
			// This case implies bestBid.Price.Add(s.Market.TickSize).Compare(bestAsk.Price) > 0
			// which means best_bid + 1 tick > best_ask. This can happen with very small positive spreads or crossed books.
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

		// Sell-side: aim to be the best ask or a competitive one.
		// Our ideal price is one tick better (lower) than the current best ask, if possible.
		potentialPriceToUndercut := bestAsk.Price.Sub(s.Market.TickSize)

		if potentialPriceToUndercut.Compare(bestBid.Price) > 0 {
			// If (bestAsk - TickSize) is still greater than bestBid,
			// we can become the new, better best ask.
			price = potentialPriceToUndercut
			log.Infof("Submitting new best ask: %s (bestAsk %s - TickSize %s > bestBid %s)",
				price.String(), bestAsk.Price.String(), s.Market.TickSize.String(), bestBid.Price.String())
		} else {
			// This means (bestAsk - TickSize) <= bestBid.
			// This occurs if:
			// 1. Spread is 1 tick: (bestAsk - TickSize) == bestBid.
			// 2. Spread is < 1 tick (but > 0): (bestAsk - TickSize) < bestBid, but bestAsk > bestBid.
			// 3. Spread is 0 or negative: bestAsk <= bestBid.

			// In cases 1 and 2, we should place at bestAsk.Price to be a maker and not cross the bid.
			// In case 3, the book is problematic.
			if bestAsk.Price.Compare(bestBid.Price) > 0 {
				// Spread is > 0 (could be 1 tick or < 1 tick). Place at current bestAsk.
				price = bestAsk.Price
				log.Infof("Spread too tight to undercut best ask. Submitting at current best ask: %s ( (bestAsk %s - TickSize %s) <= bestBid %s, and current bestAsk %s > bestBid %s)",
					price.String(), bestAsk.Price.String(), s.Market.TickSize.String(), bestBid.Price.String(), bestAsk.Price.String(), bestBid.Price.String())
			} else {
				// Spread is 0 or negative (bestAsk <= bestBid).
				return fmt.Errorf("malformed orderbook or unable to place sell order without crossing: best ask %s, best bid %s",
					bestAsk.Price.String(), bestBid.Price.String())
			}
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

	previousKnownBalance := s.lastKnownRelevantBalance // Capture the balance state before this update handling

	var currentAvailableBalance fixedpoint.Value
	var balanceSource string // For logging: "delta update" or "full fetch"
	foundRelevantBalanceInUpdate := false

	if newBalance, ok := updatedBalances[relevantCurrency]; ok {
		// Relevant currency is in the specific update event
		currentAvailableBalance = newBalance.Available
		balanceSource = "delta update"
		foundRelevantBalanceInUpdate = true
		log.Infof("[%s] Balance update for %s (from %s): New Available: %s. Old Known (before this event): %s",
			s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String(), previousKnownBalance.String())
	} else {
		// Relevant currency NOT in the specific update event.
		// This means this particular event didn't change our relevant currency, or the update is for another asset.
		// We must fetch the current absolute balance for the relevant currency to check for deposits.
		log.Debugf("[%s] Relevant currency %s not in this specific balance update event. Fetching full balance for %s to check for potential deposit.", s.Symbol, relevantCurrency, relevantCurrency)
		accountBalances := s.session.GetAccount().Balances()
		if actualBalance, found := accountBalances[relevantCurrency]; found {
			currentAvailableBalance = actualBalance.Available
			balanceSource = "full fetch"
			log.Infof("[%s] Balance status for %s (from %s): Current Available: %s. Old Known (before this event): %s",
				s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String(), previousKnownBalance.String())
		} else {
			// Relevant currency not found even in full fetch. This is unexpected.
			log.Warnf("[%s] Relevant currency %s not found in delta update NOR in full balance fetch. Last known balance %s remains unchanged. Cannot check for deposit against current state.",
				s.Symbol, relevantCurrency, previousKnownBalance.String())
			// s.lastKnownRelevantBalance remains 'previousKnownBalance'. No change can be asserted.
			return
		}
	}

	// Now, compare currentAvailableBalance (from delta or full fetch) with previousKnownBalance
	comparisonResult := currentAvailableBalance.Compare(previousKnownBalance)

	if comparisonResult > 0 {
		log.Infof("[%s] Detected deposit for %s (balance source: %s). New available balance: %s (was %s). Attempting to place order.",
			s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String(), previousKnownBalance.String())

		// Update lastKnownRelevantBalance before trying to place order
		s.lastKnownRelevantBalance = currentAvailableBalance

		if err := s.placeOrder(ctx); err != nil {
			log.WithError(err).Errorf("[%s] Error placing order after balance update: %v", s.Symbol, err)
		} else {
			log.Infof("[%s] Order placement attempt after balance update completed.", s.Symbol)
			s.lastUpdateTime = time.Now() // Reset interval timer as we've just acted
		}
	} else { // Balance decreased or stayed the same
		if comparisonResult < 0 {
			log.Infof("[%s] Balance for %s decreased (balance source: %s). New available: %s (was %s).",
				s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String(), previousKnownBalance.String())
		} else { // comparisonResult == 0
			// Only log if the balance came from a delta update and was the same, or for verbosity on full fetch.
			// Avoid spamming logs if balance is checked via full fetch and hasn't changed.
			if foundRelevantBalanceInUpdate {
				log.Infof("[%s] Balance for %s unchanged via %s. Available: %s.",
					s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String())
			} else {
				log.Debugf("[%s] Balance for %s confirmed unchanged via %s. Available: %s. No deposit detected.",
					s.Symbol, relevantCurrency, balanceSource, currentAvailableBalance.String())
			}
		}
		// Always update lastKnownRelevantBalance to the latest confirmed state
		s.lastKnownRelevantBalance = currentAvailableBalance
	}
}
