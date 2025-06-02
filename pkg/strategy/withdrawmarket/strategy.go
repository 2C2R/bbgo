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
	session.UserDataStream.OnBalanceUpdate(s.handleBalanceUpdate)

	// Perform an initial check to engage timer logic if conditions are met at startup.
	log.Infof("Performing initial balance check for %s on startup.", s.Symbol)
	initialBalances, err := s.session.Exchange.QueryAccountBalances(context.Background())
	if err != nil {
		log.Errorf("Error fetching initial balances for %s: %v. Will rely on subsequent balance updates.", s.Symbol, err)
	} else {
		s.handleBalanceUpdate(initialBalances)
	}
	return nil
}

func (s *Strategy) handleBalanceUpdate(balanceMap types.BalanceMap) {
	ctx := context.Background()

	// If a timer is active, stop it. We are processing now, either due to a real update or the timer firing.
	if s.withdrawalCheckTimer != nil {
		s.withdrawalCheckTimer.Stop()
		s.withdrawalCheckTimer = nil
	}

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
		return // No timer needed if there are open orders.
	}

	// No open orders. Manage pending state and timer.
	if !s.isPendingWithdrawal {
		// Not currently pending: Enter pending state and set a timer for the full delay.
		log.Infof("no open orders detected. Entering pending withdrawal state for %s. Will check balance after %v.", s.Symbol, s.BalanceFinalizationDelay.Duration())
		s.isPendingWithdrawal = true
		s.pendingWithdrawalSince = time.Now()

		s.withdrawalCheckTimer = time.AfterFunc(s.BalanceFinalizationDelay.Duration(), func() {
			log.Infof("Withdrawal check timer fired for %s. Fetching current balances and re-evaluating.", s.Symbol)
			currentBalances, err := s.session.Exchange.QueryAccountBalances(context.Background())
			if err != nil {
				log.Errorf("Error fetching balances in timer callback for %s: %v. Scheduling one retry.", s.Symbol, err)
				// Retry once after a short delay.
				s.withdrawalCheckTimer = time.AfterFunc(1*time.Minute, func() {
					log.Infof("Retrying withdrawal check for %s after timer fetch error.", s.Symbol)
					retryBalances, errRetry := s.session.Exchange.QueryAccountBalances(context.Background())
					if errRetry != nil {
						log.Errorf("Error fetching balances on retry in timer for %s: %v. Resetting pending state.", s.Symbol, errRetry)
						s.isPendingWithdrawal = false // Give up on timer path if retry fails
						return
					}
					s.handleBalanceUpdate(retryBalances)
				})
				return
			}
			s.handleBalanceUpdate(currentBalances)
		})
		return // Return after setting up the timer.
	}

	// If we reach here, s.isPendingWithdrawal is true.
	// This means either a real balance update occurred while pending, or a previous timer fired and called us.
	if time.Since(s.pendingWithdrawalSince) < s.BalanceFinalizationDelay.Duration() {
		// Delay has NOT passed. We are in pending state.
		// A timer was stopped at the beginning. We need to set a new timer for the *remaining* duration.
		remainingDelay := s.BalanceFinalizationDelay.Duration() - time.Since(s.pendingWithdrawalSince)

		if remainingDelay > 0 {
			log.Debugf("Still waiting for balance finalization for %s. %v remaining. Re-scheduling check.", s.Symbol, remainingDelay)
			s.withdrawalCheckTimer = time.AfterFunc(remainingDelay, func() {
				log.Infof("Withdrawal check timer (re-scheduled) fired for %s. Fetching current balances and re-evaluating.", s.Symbol)
				currentBalances, err := s.session.Exchange.QueryAccountBalances(context.Background())
				if err != nil {
					log.Errorf("Error fetching balances in re-scheduled timer callback for %s: %v. Scheduling one retry.", s.Symbol, err)
					// Retry once after a short delay.
					s.withdrawalCheckTimer = time.AfterFunc(1*time.Minute, func() {
						log.Infof("Retrying withdrawal check for %s after re-scheduled timer fetch error.", s.Symbol)
						retryBalances, errRetry := s.session.Exchange.QueryAccountBalances(context.Background())
						if errRetry != nil {
							log.Errorf("Error fetching balances on retry in re-scheduled timer for %s: %v. Resetting pending state.", s.Symbol, errRetry)
							s.isPendingWithdrawal = false
							return
						}
						s.handleBalanceUpdate(retryBalances)
					})
					return
				}
				s.handleBalanceUpdate(currentBalances)
			})
			return // Return as we've re-scheduled the timer for the remaining duration.
		}
		// If remainingDelay <= 0, it means time is effectively up. Fall through to proceed with the check.
		log.Debugf("Remaining delay for %s is zero or negative (%v). Proceeding to check.", s.Symbol, remainingDelay)
	}

	// ---- Delay has passed ----
	// This block is reached if:
	// 1. s.isPendingWithdrawal was true, AND
	// 2. time.Since(s.pendingWithdrawalSince) >= s.BalanceFinalizationDelay.Duration()
	//    (either directly, or because remainingDelay was <= 0 in the block above)
	log.Infof("pending withdrawal delay for %s has passed. Checking balances now.", s.Symbol)
	s.isPendingWithdrawal = false // Reset pending state as we are now performing the check.

	// Check if the base balance is greater than the min amount
	baseBalance := balanceMap[s.Market.BaseCurrency]
	log.Infof("base balance: %s", baseBalance.Available.String())
	if baseBalance.Available.Compare(s.Market.MinQuantity) > 0 {
		log.Infof("base currency balance %s (%s) is greater than min quantity %s, please wait for the order to be filled or manually manage.", baseBalance.Available.String(), s.Market.BaseCurrency, s.Market.MinQuantity.String())
		return
	}

	// Withdraw the available quote balance
	quoteBalance := balanceMap[s.Market.QuoteCurrency]
	log.Infof("quote balance: %s", quoteBalance.Available.String())

	if quoteBalance.Available.Compare(s.MinWithdrawAmount) < 0 {
		log.Infof("quote currency balance %s (%s) is less than min withdraw amount %s, skipping withdraw.", quoteBalance.Available.String(), s.Market.QuoteCurrency, s.MinWithdrawAmount.String())
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
