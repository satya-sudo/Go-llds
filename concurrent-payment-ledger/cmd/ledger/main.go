package main

import (
	"context"
	"fmt"
	"strings"

	ledger "concurrent-payment-ledger"
)

type TestCase struct {
	Name          string
	Accounts      map[string]int64
	Transfers     []ledger.Transfer
	Workers       int
	WantBalances  map[string]int64
	WantSuccesses int
}

func isSuccess(r ledger.TransferResult) bool {
	return strings.EqualFold(r.Status, "SUCCESS")
}

func runTest(tc TestCase) bool {
	fmt.Printf("\n--- %s ---\n", tc.Name)

	repo := ledger.NewLedgerRepo()

	for id, balance := range tc.Accounts {
		repo.AddAcount(&ledger.Account{
			ID:      id,
			Balance: balance,
		})
	}

	results := repo.ProcessBatch(
		context.Background(),
		tc.Transfers,
		tc.Workers,
	)

	passed := true
	successes := 0

	if len(results) != len(tc.Transfers) {
		fmt.Printf("FAIL: expected %d results, got %d\n",
			len(tc.Transfers), len(results))
		passed = false
	}

	for i, result := range results {
		fmt.Printf("ledger.Transfer=%s Status=%s Reason=%s\n",
			result.ID, result.Status, result.Reason)

		if i < len(tc.Transfers) &&
			result.ID != tc.Transfers[i].ID {
			fmt.Printf("FAIL: result order mismatch at %d\n", i)
			passed = false
		}

		if isSuccess(result) {
			successes++
		}
	}

	if successes != tc.WantSuccesses {
		fmt.Printf("FAIL: expected %d successes, got %d\n",
			tc.WantSuccesses, successes)
		passed = false
	}

	for id, want := range tc.WantBalances {
		got, exists := repo.GetBalance(id)

		if !exists || got != want {
			fmt.Printf("FAIL: account=%s want=%d got=%d exists=%v\n",
				id, want, got, exists)
			passed = false
		}
	}

	if passed {
		fmt.Println("PASS")
	}
	return passed
}

func main() {
	tests := []TestCase{
		{
			Name: "1. Successful transfer",
			Accounts: map[string]int64{
				"A": 1000, "B": 500,
			},
			Transfers: []ledger.Transfer{
				{ID: "T1", From: "A", To: "B", Amount: 200},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"A": 800, "B": 700},
			WantSuccesses: 1,
		},
		{
			Name: "2. Insufficient balance",
			Accounts: map[string]int64{
				"A": 100, "B": 500,
			},
			Transfers: []ledger.Transfer{
				{ID: "T2", From: "A", To: "B", Amount: 200},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"A": 100, "B": 500},
			WantSuccesses: 0,
		},
		{
			Name: "3. Sender does not exist",
			Accounts: map[string]int64{
				"B": 500,
			},
			Transfers: []ledger.Transfer{
				{ID: "T3", From: "X", To: "B", Amount: 100},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"B": 500},
			WantSuccesses: 0,
		},
		{
			Name: "4. Receiver does not exist",
			Accounts: map[string]int64{
				"A": 1000,
			},
			Transfers: []ledger.Transfer{
				{ID: "T4", From: "A", To: "X", Amount: 100},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"A": 1000},
			WantSuccesses: 0,
		},
		{
			Name: "5. Multiple independent transfers",
			Accounts: map[string]int64{
				"A": 1000, "B": 500,
				"C": 800, "D": 200,
			},
			Transfers: []ledger.Transfer{
				{ID: "T5", From: "A", To: "B", Amount: 100},
				{ID: "T6", From: "C", To: "D", Amount: 200},
				{ID: "T7", From: "A", To: "D", Amount: 300},
			},
			Workers: 4,
			WantBalances: map[string]int64{
				"A": 600, "B": 600,
				"C": 600, "D": 700,
			},
			WantSuccesses: 3,
		},
		{
			Name: "6. Concurrent withdrawals from same account",
			Accounts: map[string]int64{
				"A": 500, "B": 0,
			},
			Transfers: []ledger.Transfer{
				{ID: "T8", From: "A", To: "B", Amount: 100},
				{ID: "T9", From: "A", To: "B", Amount: 100},
				{ID: "T10", From: "A", To: "B", Amount: 100},
				{ID: "T11", From: "A", To: "B", Amount: 100},
				{ID: "T12", From: "A", To: "B", Amount: 100},
				{ID: "T13", From: "A", To: "B", Amount: 100},
				{ID: "T14", From: "A", To: "B", Amount: 100},
			},
			Workers:       7,
			WantBalances:  map[string]int64{"A": 0, "B": 500},
			WantSuccesses: 5,
		},
		{
			Name: "7. Empty batch",
			Accounts: map[string]int64{
				"A": 1000,
			},
			Transfers:     []ledger.Transfer{},
			Workers:       4,
			WantBalances:  map[string]int64{"A": 1000},
			WantSuccesses: 0,
		},
		{
			Name: "8. Zero amount rejected",
			Accounts: map[string]int64{
				"A": 1000, "B": 500,
			},
			Transfers: []ledger.Transfer{
				{ID: "T15", From: "A", To: "B", Amount: 0},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"A": 1000, "B": 500},
			WantSuccesses: 0,
		},
		{
			Name: "9. Negative amount rejected",
			Accounts: map[string]int64{
				"A": 1000, "B": 500,
			},
			Transfers: []ledger.Transfer{
				{ID: "T16", From: "A", To: "B", Amount: -100},
			},
			Workers:       2,
			WantBalances:  map[string]int64{"A": 1000, "B": 500},
			WantSuccesses: 0,
		},
	}

	passed := 0
	for _, tc := range tests {
		if runTest(tc) {
			passed++
		}
	}

	fmt.Printf("\nRESULT: %d/%d tests passed\n", passed, len(tests))
}
