package concurrentpaymentledger

import (
	"errors"
	"sync"
)

type TransactionType int

const (
	CREDIT TransactionType = iota
	DEBIT
)

var InSufficientBalanceErr = errors.New("Insufficent Balance")
var AccountDoesNotExistErr = errors.New("Acount does not exists")

type Account struct {
	ID      string
	Balance int64
	mu      sync.Mutex
}

func (a *Account) StartTransaction(val int64, action TransactionType) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if action == CREDIT {
		a.Balance += val
		return nil
	} else {
		if a.Balance < val {
			return InSufficientBalanceErr
		} else {
			a.Balance -= val
		}
	}
	return nil
}

func (a *Account) GetBalance() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.Balance
}

func StartTransactionFullTransaction(toAccount *Account, fromAccount *Account, val int64) error {
	toAccount.mu.Lock()
	fromAccount.mu.Lock()
	defer toAccount.mu.Unlock()
	defer fromAccount.mu.Unlock()

	if fromAccount.Balance < val {
		return InSufficientBalanceErr
	} else {
		fromAccount.Balance -= val
		toAccount.Balance += val
	}
	return nil
}
