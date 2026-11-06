package concurrentpaymentledger

import (
	"context"
	"errors"
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

func (l *LedgerRepository) ValidateTransaction(transactionId string) bool {
	_, exist := l.transactionHistory[transactionId]
	return exist
}

func (l *LedgerRepository) RegisterTransaction(transaction *TransferResult) {
	l.transactionHistory[transaction.ID] = transaction
}

func (l *LedgerRepository) ProcessBatch(ctx context.Context, transfers []Transfer, workers int) []TransferResult {
	workerPool := New[TransferResult](workers, 10)
	defer workerPool.Shutdown()

	for idx, transfer := range transfers {
		excuteFunc := func(ctx context.Context) (TransferResult, error) {
			to := transfer.To
			from := transfer.From
			if !l.CheckAccount(to) || l.CheckAccount(from) {
				return TransferResult{
					ID:     transfer.ID,
					Status: "failed",
					Reason: "acount does not exists",
				}, AccountDoesNotExistErr
			}
			if l.ValidateTransaction(transfer.ID) {
				return TransferResult{
					ID:     transfer.ID,
					Status: "failed",
					Reason: "Duplicate transaction",
				}, errors.New("Duplicate transaction")
			}
			toAccount, _ := l.accounts[to]
			fromAccount, _ := l.accounts[from]
			err := StartTransactionFullTransaction(toAccount, fromAccount, transfer.Amount)
			if err != nil {
				return TransferResult{
					ID:     transfer.ID,
					Status: "failed",
					Reason: err.Error(),
				}, err
			}
			result := TransferResult{
				ID:     transfer.ID,
				Status: "Success",
				Reason: "success",
			}
			l.RegisterTransaction(&result)

			return result, nil
		}
		job := Job[TransferResult]{ID: transfer.ID, Index: idx, Execute: excuteFunc}
		workerPool.Submit(ctx, job)
	}
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
