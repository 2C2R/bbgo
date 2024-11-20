package liquidated

import (
	"context"
	"fmt"
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

	Market types.Market

	orderExecutor *bbgo.GeneralOrderExecutor
	session       *bbgo.ExchangeSession
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	// 不需要訂閱任何資料
}

func (s *Strategy) placeOrder(ctx context.Context) error {
	// 檢查交易方向
	side := types.SideTypeBuy
	if s.Side == "SELL" {
		side = types.SideTypeSell
	}

	// 取得帳戶餘額
	balances := s.session.GetAccount().Balances()

	// 取得最新報價
	ticker, err := s.session.Exchange.QueryTicker(ctx, s.Symbol)
	if err != nil {
		return err
	}

	// 根據交易方向決定要用哪個餘額與價格
	var quantity fixedpoint.Value
	var price fixedpoint.Value

	if side == types.SideTypeBuy {
		// 買入時用 quote currency 餘額
		quoteBalance, ok := balances[s.Market.QuoteCurrency]
		if !ok {
			return fmt.Errorf("quote balance %s not found", s.Market.QuoteCurrency)
		}

		if quoteBalance.Available.Compare(s.MinQuoteBalance) <= 0 {
			return fmt.Errorf("quote balance %s less than min balance %s",
				quoteBalance.Available.String(), s.MinQuoteBalance.String())
		}

		// 取得買入價格
		price = s.PriceType.GetPrice(ticker, side)
		if s.OffsetTick > 0 {
			price = price.Sub(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}

		// 計算可買入數量 = 可用餘額 / 當前價格
		quantity = quoteBalance.Available.Div(price)

	} else {
		// 賣出時用 base currency 餘額
		baseBalance, ok := balances[s.Market.BaseCurrency]
		if !ok {
			return fmt.Errorf("base balance %s not found", s.Market.BaseCurrency)
		}
		quantity = baseBalance.Available

		// 取得賣出價格
		price = s.PriceType.GetPrice(ticker, side)
		if s.OffsetTick > 0 {
			price = price.Add(s.Market.TickSize.Mul(fixedpoint.NewFromInt(int64(s.OffsetTick))))
		}
	}

	// 檢查數量是否符合最小交易量
	if quantity.Compare(s.Market.MinQuantity) < 0 {
		return fmt.Errorf("quantity %s is less than min quantity %s",
			quantity.String(), s.Market.MinQuantity.String())
	}

	// 先取消之前的掛單
	if err := s.orderExecutor.GracefulCancel(ctx); err != nil {
		log.WithError(err).Errorf("cannot cancel orders")
	}

	if s.DryRun {
		log.Infof("[DryRun] would submit order: Symbol=%s Side=%s Type=%s Price=%s Quantity=%s TimeInForce=%s",
			s.Symbol, side, types.OrderTypeLimitMaker, price.String(), quantity.String(), types.TimeInForceGTC)
		return nil
	}

	// 送出限價掛單
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

func (s *Strategy) Run(ctx context.Context, orderExecutor *bbgo.GeneralOrderExecutor, session *bbgo.ExchangeSession) error {
	s.orderExecutor = orderExecutor
	s.session = session

	market, ok := session.Market(s.Symbol)
	if !ok {
		return fmt.Errorf("market %s not found", s.Symbol)
	}
	s.Market = market

	// 設定預設值
	if s.UpdateInterval == 0 {
		s.UpdateInterval = types.Duration(time.Second * 5)
	}
	if s.PriceType == "" {
		s.PriceType = types.PriceTypeMaker
	}

	ticker := time.NewTicker(s.UpdateInterval.Duration())
	defer ticker.Stop()

	// 先執行一次
	if err := s.placeOrder(ctx); err != nil {
		log.WithError(err).Error("cannot place order")
	}

	// 定時檢查並更新掛單
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
