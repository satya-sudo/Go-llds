package concurrentpaymentledger

import (
	"context"
	"errors"
	"sync"
)

type Transfer struct {
	ID     string
	From   string
	To     string
	Amount int64
}

type TransferResult struct {
	ID     string
	Status string
	Reason string
}

type Ledger interface {
	ProcessBatch(ctx context.Context, transfers []Transfer, workers int) []TransferResult
	GetBalance(accountID string) (int64, bool)
}

type LedgerRepository struct {
	accounts           map[string]*Account
	transactionHistory map[string]*TransferResult
	mu                 sync.Mutex
}

func NewLedgerRepo() *LedgerRepository {
	return &LedgerRepository{
		accounts:           map[string]*Account{},
		transactionHistory: map[string]*TransferResult{},
	}
}

func (l *LedgerRepository) AddAcount(account *Account) bool {
	_, exists := l.accounts[account.ID]
	if exists {
		return false
	}
	l.accounts[account.ID] = account
	return true
}
func (l *LedgerRepository) CheckAccount(accountId string) bool {
	_, exists := l.accounts[accountId]
	return exists
}

func (l *LedgerRepository) StartTransaction(transactionId string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, exist := l.transactionHistory[transactionId]
	if exist {
		return false
	}
	l.transactionHistory[transactionId] = &TransferResult{
		ID:     transactionId,
		Status: "IN_PROGRESS",
		Reason: "",
	}
	return true
}

func (l *LedgerRepository) RegisterTransaction(transaction *TransferResult) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.transactionHistory[transaction.ID] = transaction
}

func (l *LedgerRepository) ProcessBatch(ctx context.Context, transfers []Transfer, workers int) []TransferResult {
	workerPool := New[TransferResult](workers, 10)
	go func() {
		for idx, transfer := range transfers {
			excuteFunc := func(ctx context.Context) (TransferResult, error) {
				to := transfer.To
				from := transfer.From
				result := TransferResult{
					ID:     transfer.ID,
					Status: "IN_PROGRESS",
					Reason: "",
				}
				var err error
				if to == from {
					result.Status = "FAILED"
					result.Reason = "Self transfer"
					err = SelfTransferErr
				} else if transfer.Amount <= 0 {
					result.Status = "FAILED"
					result.Reason = "Invalid amount"
					err = InvalidAmountErr
				} else if !l.CheckAccount(to) || !l.CheckAccount(from) {
					result.Status = "FAILED"
					result.Reason = "acount does not exists"
					err = AccountDoesNotExistErr
				} else if !l.StartTransaction(transfer.ID) {
					result.Status = "FAILED"
					result.Reason = "Duplicate transaction"
					err = errors.New("Duplicate transaction")
				} else {
					toAccount, _ := l.accounts[to]
					fromAccount, _ := l.accounts[from]
					err = StartTransactionFullTransaction(toAccount, fromAccount, transfer.Amount)
					if err != nil {
						result.Status = "FAILED"
						result.Reason = err.Error()
					} else {
						result.Status = "SUCCESS"
					}
				}
				if err != nil && err.Error() == "Duplicate transaction" {
					return result, err
				} else {
					l.RegisterTransaction(&result)
				}
				return result, err

			}
			job := Job[TransferResult]{ID: transfer.ID, Index: idx, Execute: excuteFunc}
			workerPool.Submit(ctx, job)
		}
		workerPool.Shutdown()
		close(workerPool.results)
	}()
	results := make([]TransferResult, len(transfers))
	for result := range workerPool.results {
		results[result.Index] = result.Value
	}

	return results
}

func (l *LedgerRepository) GetBalance(accountID string) (int64, bool) {
	account, ok := l.accounts[accountID]
	if !ok {
		return 0, false
	}

	return account.GetBalance(), true
}
