Concurrent Payment Processing Engine
Medium–Hard
Mutex
Goroutines
Worker Pool

The situation
You're developing the payment processing service for a fintech company.

Payment requests arrive in batches. Multiple workers must process them concurrently, but all account balances must remain consistent.

The system must handle duplicate requests, insufficient balances, and concurrent transfers between the same accounts.
Initial account balances
Account ID	Balance (₹)
A	1,000
B	500
C	200
D	100

The data structures
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


You are free to design the internal structures and locking strategy.
Test dataset
Batch 1 — process these concurrently
ID	From	To	Amount
T1	A	B	300
T2	C	D	100
T3	A	B	100
T4	A	C	200

Batch 2 — process after Batch 1 completes
ID	From	To	Amount
T5	A	D	450
T6	B	C	250
T7	D	B	50
T1	A	B	300
T8	C	A	-100

Requirements
1. Use a worker pool with a configurable number of workers.
2. Each account must have its own mutex. Do not lock the entire ledger while processing every transaction.
3. A transfer must be atomic: money must never be debited without being credited.
4. Reject insufficient funds, negative/zero amounts, invalid account IDs, and self-transfers.
5. Duplicate transaction IDs must never be applied twice, even when two workers process the same ID simultaneously.
6. Prevent deadlocks when A sends to B while B sends to A.
7. Support context cancellation and ensure all worker goroutines exit.
8. GetBalance() must be safe while transfers are executing.
9. Return a result for every submitted request, including requests skipped because of cancellation.
Important: Transfers within a batch may execute in any order. A successful transfer's effect must be visible to later transfers, but you don't need to preserve input order during execution. Return results in input order.
Expected results
After Batch 1, balances must be:
Account	Balance
A	₹400
B	₹900
C	₹300
D	₹200

For Batch 2, T5 fails due to insufficient funds, T1 is a duplicate, and T8 is invalid. The other two transfers succeed.

Final balances:
Account	Balance
A	₹400
B	₹700
C	₹550
D	₹150

Total must remain ₹1,800.
Interviewer follow-ups
When you finish, I'll challenge your design:
- How do you acquire multiple mutexes without introducing deadlocks?
- What happens when 100 workers receive the same transaction ID?
- If cancellation arrives between debit and credit, what should happen?
- How would you safely implement GetAllBalances() to return a consistent snapshot?
- What if the service crashes immediately after recording a transaction as processed?
Hint available on request. Your first challenge is deciding how to coordinate idempotency checks with account-level locking.

Part 2 — SQL: Customer Spending Analysis
15–20 minutes
PostgreSQL

Tables
customers
customer_id	name
1	Alice
2	Bob
3	Charlie
4	David

orders
order_id	customer_id	amount	status
101	1	500	completed
102	1	700	completed
103	1	300	cancelled
104	2	900	completed
105	2	400	completed
106	3	200	completed
107	4	800	cancelled
108	3	600	completed
109	2	100	refunded
110	1	200	completed