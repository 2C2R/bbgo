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
	// BalanceFinalizationDelay is the duration to wait for balance to be finalized
	BalanceFinalizationDelay types.Duration `json:"balanceFinalizationDelay"`

	// Market stores the market configuration of the symbol
	Market types.Market
	// Position stores the position of the symbol
	Position *types.Position

	// session is the exchange session
	session *bbgo.ExchangeSession
	// orderExecutor is the exchange order executor
	orderExecutor *bbgo.GeneralOrderExecutor

	// isPendingWithdrawal indicates if the strategy is waiting for balance finalization
	isPendingWithdrawal bool
	// pendingWithdrawalSince stores the time when the strategy entered pending withdrawal state
	pendingWithdrawalSince time.Time
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

	if s.BalanceFinalizationDelay <= 0 {
		s.BalanceFinalizationDelay = types.Duration(3 * time.Minute)
		log.Infof("BalanceFinalizationDelay is not set, using default: %v", s.BalanceFinalizationDelay)
	}

	// Subscribe to balance updates
	session.UserDataStream.OnBalanceUpdate(s.handleBalanceUpdate)
	return nil
}

func (s *Strategy) handleBalanceUpdate(balanceMap types.BalanceMap) {
	ctx := context.Background()

	// Check for open orders first
	service, ok := s.session.Exchange.(types.ExchangeTradeService)
	if !ok {
		log.Errorf("exchange %s does not implement order query service", s.session.ExchangeName)
		s.isPendingWithdrawal = false // Reset pending state if service is not available
		return
	}
	orders, err := service.QueryOpenOrders(ctx, s.Symbol)
	if err != nil {
		log.Errorf("failed to query orders: %v", err)
		s.isPendingWithdrawal = false // Reset pending state on error
		return
	}

	if len(orders) > 0 {
		if s.isPendingWithdrawal {
			log.Info("open orders detected, cancelling pending withdraw check")
			s.isPendingWithdrawal = false
		}
		return
	}

	// No open orders
	if !s.isPendingWithdrawal {
		log.Infof("no open orders detected. Entering pending withdrawal state for %s. Will check balance after %v.", s.Symbol, s.BalanceFinalizationDelay.Duration())
		s.isPendingWithdrawal = true
		s.pendingWithdrawalSince = time.Now()
		return
	}

	// Currently in pending withdrawal state
	if time.Since(s.pendingWithdrawalSince) < s.BalanceFinalizationDelay.Duration() {
		// log.Debugf("still waiting for balance finalization for %s...", s.Symbol) // Optional: for debugging
		return
	}

	log.Infof("pending withdrawal delay for %s has passed. Checking balances now.", s.Symbol)
	// Reset pending state, actual check will happen now.
	// If withdrawal happens, great. If not, it will re-enter pending state if conditions (no open orders) are met again.
	s.isPendingWithdrawal = false

	// Check if the base balance is greater than the min amount
	baseBalance := balanceMap[s.Market.BaseCurrency]
	logrus.Infof("base balance: %v", baseBalance)
	if baseBalance.Available.Compare(s.Market.MinQuantity) > 0 {
		log.Infof("base currency balance %s (%s) is greater than min quantity %s, please wait for the order to be filled or manually manage.", baseBalance.Available, s.Market.BaseCurrency, s.Market.MinQuantity)
		return
	}

	// Withdraw the available quote balance
	quoteBalance := balanceMap[s.Market.QuoteCurrency]
	logrus.Infof("quote balance: %v", quoteBalance)

	if quoteBalance.Available.Compare(s.MinWithdrawAmount) < 0 {
		log.Infof("quote currency balance %s (%s) is less than min withdraw amount %s, skipping withdraw.", quoteBalance.Available, s.Market.QuoteCurrency, s.MinWithdrawAmount)
		return
	}

	log.Infof("conditions met for withdrawal for %s. Proceeding to submit.", s.Symbol)
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
