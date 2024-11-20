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
	Symbol          string           `json:"symbol"`
	Side            string           `json:"side"`
	PriceType       types.PriceType  `json:"priceType"`
	UpdateInterval  types.Duration   `json:"updateInterval"`
	OffsetTick      int              `json:"offsetTick"`
	MinQuoteBalance fixedpoint.Value `json:"minQuoteBalance"`
	DryRun          bool             `json:"dryRun"`

	Market   types.Market
	Position *types.Position `json:"-"`

	session       *bbgo.ExchangeSession
	orderExecutor *bbgo.GeneralOrderExecutor
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	// No need to subscribe any data
}

func (s *Strategy) placeOrder(ctx context.Context) error {
	// Check trading direction
	side := types.SideTypeBuy
	if s.Side == "SELL" {
		side = types.SideTypeSell
	}

	// Get account balances
	balances := s.session.GetAccount().Balances()

	// Get latest ticker
	ticker, err := s.session.Exchange.QueryTicker(ctx, s.Symbol)
	if err != nil {
		return err
	}

	// Determine which balance and price to use based on trading direction
	var quantity fixedpoint.Value
	var price fixedpoint.Value

	if side == types.SideTypeBuy {
		// Use quote currency balance for BUY
		quoteBalance, ok := balances[s.Market.QuoteCurrency]
		if !ok {
			return fmt.Errorf("quote balance %s not found", s.Market.QuoteCurrency)
		}

		if quoteBalance.Available.Compare(s.MinQuoteBalance) <= 0 {
			return fmt.Errorf("quote balance %s less than min balance %s",
				quoteBalance.Available.String(), s.MinQuoteBalance.String())
		}

		// Get buy price
		price = s.PriceType.GetPrice(ticker, side)
		if s.OffsetTick > 0 {
			price = price.Sub(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}

		// Calculate buy quantity = available balance / current price
		quantity = quoteBalance.Available.Div(price)

	} else {
		// Use base currency balance for SELL
		baseBalance, ok := balances[s.Market.BaseCurrency]
		if !ok {
			return fmt.Errorf("base balance %s not found", s.Market.BaseCurrency)
		}
		quantity = baseBalance.Available

		// Get sell price
		price = s.PriceType.GetPrice(ticker, side)
		if s.OffsetTick > 0 {
			price = price.Add(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}
	}

	// Check quantity is greater than min quantity
	if quantity.Compare(s.Market.MinQuantity) < 0 {
		return fmt.Errorf("quantity %s is less than min quantity %s",
			quantity.String(), s.Market.MinQuantity.String())
	}

	// Check if there are existing orders with the same price
	activeOrders := s.orderExecutor.ActiveMakerOrders()
	for _, o := range activeOrders.Orders() {
		if o.Price.Compare(price) == 0 {
			log.Infof("skip placing order: existing order found at price %s", price.String())
			return nil
		}
	}

	// Cancel existing orders with a delay to ensure they are cancelled
	if err := s.orderExecutor.GracefulCancel(ctx); err != nil {
		log.WithError(err).Errorf("cannot cancel orders")
		return err
	}

	// Verify no active orders remain
	if s.orderExecutor.ActiveMakerOrders().NumOfOrders() > 0 {
		return fmt.Errorf("there are still active orders that could not be cancelled")
	}

	if s.DryRun {
		log.Infof("[DryRun] would submit order: Symbol=%s Side=%s Type=%s Price=%s Quantity=%s TimeInForce=%s",
			s.Symbol, side, types.OrderTypeLimitMaker, price.String(), quantity.String(), types.TimeInForceGTC)
		return nil
	}

	// Submit limit maker order
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

	log.Infof("created orders: %+v", createdOrders)
	return nil
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	s.session = session
	if s.Position == nil {
		s.Position = types.NewPosition(s.Symbol, s.Market.BaseCurrency, s.Market.QuoteCurrency)
	}
	s.orderExecutor = bbgo.NewGeneralOrderExecutor(session, s.Symbol, ID, "", s.Position)

	// Get market info
	market, ok := session.Market(s.Symbol)
	if !ok {
		return fmt.Errorf("market %s not found", s.Symbol)
	}
	s.Market = market

	// Set default values
	if s.UpdateInterval == 0 {
		s.UpdateInterval = types.Duration(time.Second * 5)
	}
	if s.PriceType == "" {
		s.PriceType = types.PriceTypeMaker
	}

	// Wait for user data stream to be ready
	session.UserDataStream.OnStart(func() {
		if err := s.placeOrder(ctx); err != nil {
			log.WithError(err).Error("cannot place order")
		}
	})

	ticker := time.NewTicker(s.UpdateInterval.Duration())
	defer ticker.Stop()

	// the shutdown handler, you can cancel all orders
	bbgo.OnShutdown(ctx, func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		_ = s.orderExecutor.GracefulCancel(ctx)
		bbgo.Sync(ctx, s)
	})

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			if err := s.placeOrder(ctx); err != nil {
				log.WithError(err).Error("cannot place order")
			}
		}
	}
}
