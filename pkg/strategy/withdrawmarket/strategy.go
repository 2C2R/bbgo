package withdrawmarket

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

const ID = "withdrawmarket"

var log = logrus.WithField("strategy", ID)

func init() {
	bbgo.RegisterStrategy(ID, &Strategy{})
}

type Strategy struct {
	// Symbol is the symbol of the market to check and withdraw
	Symbol string `json:"symbol"`
	// Address to withdraw to
	Address string `json:"address"`
	// MinWithdrawAmount is the minimum amount to withdraw
	MinWithdrawAmount fixedpoint.Value `json:"minWithdrawAmount"`
	// DryRun is a flag to indicate if the strategy is running in dry run mode
	DryRun bool `json:"dryRun"`

	// Market stores the market configuration of the symbol
	Market types.Market
	// Position stores the position of the symbol
	Position *types.Position

	// session is the exchange session
	session *bbgo.ExchangeSession
	// orderExecutor is the exchange order executor
	orderExecutor *bbgo.GeneralOrderExecutor
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	// No subscriptions needed
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	s.session = session
	if s.Position == nil {
		s.Position = types.NewPosition(s.Symbol, s.Market.BaseCurrency, s.Market.QuoteCurrency)
	}
	s.orderExecutor = bbgo.NewGeneralOrderExecutor(session, s.Symbol, ID, "", s.Position)
	market, ok := session.Market(s.Symbol)
	if !ok {
		return fmt.Errorf("market %s not found", s.Symbol)
	}
	s.Market = market

	// Subscribe to balance updates
	session.UserDataStream.OnBalanceUpdate(s.handleBalanceUpdate)
	return nil
}

func (s *Strategy) handleBalanceUpdate(balanceMap types.BalanceMap) {
	// FIXME: this is a temporary fix to avoid balance stuck issue
	time.Sleep(10 * time.Second)
	ctx := context.Background()
	// Check if the base balance is greater than the min amount
	baseBalance := balanceMap[s.Market.BaseCurrency]
	logrus.Infof("base balance: %v", baseBalance)
	if baseBalance.Available.Compare(s.Market.MinQuantity) > 0 {
		log.Infof("balance %s is greater than min quantity %s, please wait for the order to be filled", baseBalance.Available, s.Market.MinQuantity)
		return
	}

	// Withdraw the available quote balance
	quoteBalance := balanceMap[s.Market.QuoteCurrency]
	logrus.Infof("quote balance: %v", quoteBalance)

	if quoteBalance.Available.Compare(s.MinWithdrawAmount) < 0 {
		log.Infof("balance %s is less than min withdraw amount %s, skipping withdraw", quoteBalance.Available, s.MinWithdrawAmount)
		return
	}

	// Check for open orders first
	service, ok := s.session.Exchange.(types.ExchangeTradeService)
	if !ok {
		log.Errorf("exchange %s does not implement order query service", s.session.ExchangeName)
		return
	}
	orders, err := service.QueryOpenOrders(ctx, s.Symbol)
	if err != nil {
		log.Errorf("failed to query orders: %v", err)
		return
	}
	if orders != nil {
		log.Info("there are still open orders, skipping withdraw check")
		return
	}

	if err := s.submitWithdraw(ctx, balanceMap); err != nil {
		log.Errorf("failed to submit withdraw: %v", err)
	}
}

func (s *Strategy) submitWithdraw(ctx context.Context, balanceMap types.BalanceMap) error {
	withdrawService, ok := s.session.Exchange.(types.ExchangeWithdrawalService)
	if !ok {
		return fmt.Errorf("exchange %s does not implement withdrawal service", s.session.ExchangeName)
	}

	if s.DryRun {
		log.Infof("[DryRun] would submit withdraw request: Symbol=%s Currency=%s Amount=%s Address=%s",
			s.Symbol, s.Market.QuoteCurrency, balanceMap[s.Market.QuoteCurrency].Available.String(), s.Address)
		return nil
	}
	// Floor the amount to the nearest integer
	withdrawAmount := balanceMap[s.Market.QuoteCurrency].Available.Floor()
	log.Infof("submitting withdraw request: Symbol=%s Currency=%s Amount=%s Address=%s",
		s.Symbol, s.Market.QuoteCurrency, withdrawAmount.String(), s.Address)
	if err := withdrawService.Withdraw(ctx, s.Market.QuoteCurrency, withdrawAmount, s.Address, &types.WithdrawalOptions{}); err != nil {
		return err
	}
	bbgo.Notify("Withdrawed %s %s to address %s", withdrawAmount.String(), s.Market.QuoteCurrency, s.Address)
	return nil
}
