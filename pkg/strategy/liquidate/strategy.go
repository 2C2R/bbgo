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
	Symbol         string         `json:"symbol"`
	Side           string         `json:"side"`
	UpdateInterval types.Duration `json:"updateInterval"`
	OffsetTick     int            `json:"offsetTick"`
	DryRun         bool           `json:"dryRun"`

	Market   types.Market
	Position *types.Position

	session        *bbgo.ExchangeSession
	orderExecutor  *bbgo.GeneralOrderExecutor
	orderBook      *types.StreamOrderBook
	lastUpdateTime time.Time
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
		priceVolume, ok := ob.BestBid()
		if !ok {
			return fmt.Errorf("best bid not found")
		}
		price = priceVolume.Price
		log.Infof("best bid: %s", price.String())
		if s.OffsetTick > 0 {
			price = price.Sub(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}
	} else {
		priceVolume, ok := ob.BestAsk()
		if !ok {
			return fmt.Errorf("best ask not found")
		}
		price = priceVolume.Price
		log.Infof("best ask: %s", price.String())
		if s.OffsetTick > 0 {
			price = price.Add(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
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

	// the shutdown handler, you can cancel all orders
	bbgo.OnShutdown(ctx, func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		_ = s.orderExecutor.GracefulCancel(ctx)
		bbgo.Sync(ctx, s)
	})

	return nil
}
