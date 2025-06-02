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
	// withdrawalCheckTimer is used to wake up the strategy to check for withdrawal conditions
	withdrawalCheckTimer *time.Timer
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
	session.UserDataStream.OnBalanceUpdate(s.processBalanceUpdateEvent)

	// Perform an initial check to engage timer logic if conditions are met at startup.
	log.Infof("Performing initial balance check for %s on startup.", s.Symbol)
	initialBalances, err := s.fetchBalancesWithRetry(ctx)
	if err != nil {
		log.Errorf("Error fetching initial balances for %s: %v. Will rely on subsequent balance updates.", s.Symbol, err)
	} else {
		s.processBalanceUpdateEvent(initialBalances)
	}
	return nil
}

func (s *Strategy) fetchBalancesWithRetry(ctx context.Context) (types.BalanceMap, error) {
	balances, err := s.session.Exchange.QueryAccountBalances(ctx)
	if err != nil {
		log.Warnf("Failed to fetch balances for %s (attempt 1): %v. Retrying in 10s...", s.Symbol, err)
		time.Sleep(10 * time.Second) // Brief pause before retry
		balances, err = s.session.Exchange.QueryAccountBalances(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch balances for %s after retry: %w", s.Symbol, err)
		}
	}
	return balances, nil
}

func (s *Strategy) checkOpenOrders(ctx context.Context) (bool, error) {
	service, ok := s.session.Exchange.(types.ExchangeTradeService)
	if !ok {
		return false, fmt.Errorf("exchange %s does not implement order query service", s.session.ExchangeName)
	}
	orders, err := service.QueryOpenOrders(ctx, s.Symbol)
	if err != nil {
		return false, fmt.Errorf("failed to query open orders for %s: %w", s.Symbol, err)
	}
	return len(orders) > 0, nil
}

func (s *Strategy) cancelPendingWithdrawal(reason string) {
	if s.withdrawalCheckTimer != nil {
		s.withdrawalCheckTimer.Stop()
		s.withdrawalCheckTimer = nil
	}
	if s.isPendingWithdrawal {
		log.Infof("Cancelling pending withdrawal for %s. Reason: %s", s.Symbol, reason)
		s.isPendingWithdrawal = false
	}
}

func (s *Strategy) scheduleWithdrawalCheck(ctx context.Context, delay time.Duration) {
	if s.withdrawalCheckTimer != nil {
		s.withdrawalCheckTimer.Stop()
	}
	log.Infof("Scheduling withdrawal check for %s after %v.", s.Symbol, delay)
	s.withdrawalCheckTimer = time.AfterFunc(delay, func() {
		s.handleWithdrawalTimerCallback(ctx)
	})
}

func (s *Strategy) enterPendingWithdrawalState(ctx context.Context) {
	log.Infof("No open orders detected for %s. Entering pending withdrawal state. Will check balance after %v.", s.Symbol, s.BalanceFinalizationDelay.Duration())
	s.isPendingWithdrawal = true
	s.pendingWithdrawalSince = time.Now()
	s.scheduleWithdrawalCheck(ctx, s.BalanceFinalizationDelay.Duration())
}

func (s *Strategy) processBalanceUpdateEvent(balanceMap types.BalanceMap) {
	ctx := context.Background() // Using Background context for events not directly tied to Run's lifecycle

	s.cancelPendingWithdrawal("New balance update received, re-evaluating.")

	hasOpenOrders, err := s.checkOpenOrders(ctx)
	if err != nil {
		log.Errorf("Error checking open orders during balance update: %v. Aborting withdrawal check.", err)
		// No need to call cancelPendingWithdrawal again as it's done above.
		return
	}

	if hasOpenOrders {
		log.Infof("Open orders detected for %s. Withdrawal process will not proceed.", s.Symbol)
		// cancelPendingWithdrawal was already called, so state is clean.
		return
	}

	// No open orders.
	if !s.isPendingWithdrawal { // This check is a bit redundant if cancelPendingWithdrawal fully resets, but safe.
		s.enterPendingWithdrawalState(ctx)
	} else {
		// This case implies that `isPendingWithdrawal` was true, but `cancelPendingWithdrawal` above should have reset it.
		// However, if we want to handle a scenario where an update comes for a *still pending* state (e.g., timer logic gets more complex),
		// this is where we'd calculate remaining delay.
		// For now, given `cancelPendingWithdrawal` is always called, we should effectively re-enter pending state.
		log.Warnf("Balance update received for %s while isPendingWithdrawal was unexpectedly true. Re-entering pending state.", s.Symbol)
		s.enterPendingWithdrawalState(ctx) // Re-enter to ensure timer is correctly set for full duration.
	}
}

func (s *Strategy) handleWithdrawalTimerCallback(ctx context.Context) {
	log.Infof("Withdrawal check timer fired for %s. Verifying conditions.", s.Symbol)
	s.withdrawalCheckTimer = nil // Timer has fired, clear it.

	hasOpenOrders, err := s.checkOpenOrders(ctx)
	if err != nil {
		log.Errorf("Timer: Error checking open orders for %s: %v. Resetting pending state.", s.Symbol, err)
		s.cancelPendingWithdrawal("Error checking open orders in timer.")
		return
	}

	if hasOpenOrders {
		log.Infof("Timer: Open orders detected for %s after timer fired. Cancelling withdrawal.", s.Symbol)
		s.cancelPendingWithdrawal("Open orders found by timer.")
		return
	}

	// No open orders, delay has passed. Fetch current balances to proceed.
	currentBalances, err := s.fetchBalancesWithRetry(ctx)
	if err != nil {
		log.Errorf("Timer: Error fetching balances for %s: %v. Resetting pending state.", s.Symbol, err)
		s.cancelPendingWithdrawal("Failed to fetch balances in timer.")
		return
	}

	s.executePendingWithdrawal(ctx, currentBalances)
}

func (s *Strategy) executePendingWithdrawal(ctx context.Context, balanceMap types.BalanceMap) {
	log.Infof("Pending withdrawal delay for %s has passed, and no open orders. Checking balances now.", s.Symbol)
	// isPendingWithdrawal is set to false by the caller (either timer or a direct execution path if added later)
	// or by cancelPendingWithdrawal if conditions change. Here we assume it's okay to proceed.
	s.isPendingWithdrawal = false // Explicitly ensure it's false before proceeding.

	baseBalance := balanceMap[s.Market.BaseCurrency]
	log.Infof("Base balance for %s: %s", s.Symbol, baseBalance.Available.String())
	if baseBalance.Available.Compare(s.Market.MinQuantity) > 0 {
		log.Infof("Base currency balance %s (%s) for %s is greater than min quantity %s. Manual management needed or wait for fill.",
			baseBalance.Available.String(), s.Market.BaseCurrency, s.Symbol, s.Market.MinQuantity.String())
		return
	}

	quoteBalance := balanceMap[s.Market.QuoteCurrency]
	log.Infof("Quote balance for %s: %s", s.Symbol, quoteBalance.Available.String())

	if quoteBalance.Available.Compare(s.MinWithdrawAmount) < 0 {
		log.Infof("Quote currency balance %s (%s) for %s is less than min withdraw amount %s. Skipping withdraw.",
			quoteBalance.Available.String(), s.Market.QuoteCurrency, s.Symbol, s.MinWithdrawAmount.String())
		return
	}

	log.Infof("Conditions met for withdrawal for %s. Proceeding to submit.", s.Symbol)
	if err := s.submitWithdraw(ctx, balanceMap); err != nil {
		log.Errorf("Failed to submit withdraw for %s: %v", s.Symbol, err)
	}
}

func (s *Strategy) submitWithdraw(ctx context.Context, balanceMap types.BalanceMap) error {
	withdrawService, ok := s.session.Exchange.(types.ExchangeWithdrawalService)
	if !ok {
		return fmt.Errorf("exchange %s does not implement withdrawal service", s.session.ExchangeName)
	}

	if s.DryRun {
		log.Infof("[DryRun] Would submit withdraw request: Symbol=%s Currency=%s Amount=%s Address=%s",
			s.Symbol, s.Market.QuoteCurrency, balanceMap[s.Market.QuoteCurrency].Available.String(), s.Address)
		return nil
	}
	withdrawAmount := balanceMap[s.Market.QuoteCurrency].Available.Floor()
	log.Infof("Submitting withdraw request: Symbol=%s Currency=%s Amount=%s Address=%s",
		s.Symbol, s.Market.QuoteCurrency, withdrawAmount.String(), s.Address)

	// Ensure withdrawal amount is not zero or negative if Floor() can result in that
	if withdrawAmount.Sign() <= 0 {
		log.Warnf("Withdrawal amount for %s is zero or negative (%s) after flooring. Skipping withdrawal.", s.Symbol, withdrawAmount.String())
		return nil
	}

	if err := withdrawService.Withdraw(ctx, s.Market.QuoteCurrency, withdrawAmount, s.Address, &types.WithdrawalOptions{}); err != nil {
		return fmt.Errorf("withdraw service failed for %s: %w", s.Symbol, err)
	}
	bbgo.Notify("Withdrawal Submitted: %s %s of %s to address %s", withdrawAmount.String(), s.Market.QuoteCurrency, s.Symbol, s.Address)
	return nil
}
