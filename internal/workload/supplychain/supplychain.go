// Package supplychain is a demo workload: a warehouse agent that checks stock and reorders from a
// vendor when something runs low.
//
// It exists to make the harness watchable. SWE-bench is what justifies the sandbox and produces the
// isolation benchmark; this workload produces nothing except an audience that understands what
// checkpointing is for, which is why it reads inventory and sends purchase orders rather than
// running untrusted code.
//
// It owns its own tables and its own connection pool. The control plane's store holds run state and
// knows nothing about vitamins, which is the property Milestone 7 asks for: a workload arrives by
// registering step kinds, not by changing the engine.
package supplychain

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"

	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/slmatthiesen/bulkhead/internal/executor"
)

// Step kinds this workload adds.
const (
	KindQuery = "query"
	KindEmail = "email"
)

//go:embed schema.sql
var schema string

//go:embed seed.sql
var seed string

// Store holds the workload's data.
type Store struct {
	pool *pgxpool.Pool

	// StepDelay slows every step down so a run is watchable. Real queries and inserts finish in
	// single-digit milliseconds, which is correct and useless to demonstrate -- the replay-versus-
	// execute contrast is only visible if executing takes long enough to see.
	StepDelay time.Duration
}

// Open connects, creates the workload's tables, and seeds them. Seeding is idempotent and resets
// stock levels, so a demo can be re-run without hand-editing the database between takes.
func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("supplychain pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("supplychain ping: %w", err)
	}
	for _, stmt := range []string{schema, seed} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			pool.Close()
			return nil, fmt.Errorf("supplychain migrate: %w", err)
		}
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Reseed restores starting stock levels and clears the outbox, so the demo can be run again.
func (s *Store) Reseed(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, "delete from purchase_orders"); err != nil {
		return fmt.Errorf("clear outbox: %w", err)
	}
	_, err := s.pool.Exec(ctx, seed)
	return err
}

// InventoryRow is one product, as the agent sees it.
type InventoryRow struct {
	SKU              string `json:"sku"`
	Name             string `json:"name"`
	Category         string `json:"category"`
	OnHand           int    `json:"on_hand"`
	ReorderThreshold int    `json:"reorder_threshold"`
	ReorderQuantity  int    `json:"reorder_quantity"`
	UnitCostCents    int    `json:"unit_cost_cents"`
	DailyUsage       int    `json:"daily_usage"`
	VendorID         string `json:"vendor_id"`
	VendorName       string `json:"vendor_name"`
	VendorEmail      string `json:"vendor_email"`
}

// QueryInput names a query rather than carrying SQL.
//
// The agent picks from a fixed set instead of composing statements. An agent that emits raw SQL is
// a different demo, one about prompt injection, and it would put the interesting failure in the
// wrong place.
type QueryInput struct {
	Query    string `json:"query"`
	Category string `json:"category,omitempty"`
}

type QueryOutput struct {
	Query string         `json:"query"`
	Count int            `json:"count"`
	Rows  []InventoryRow `json:"rows"`
}

const inventorySelect = `
select i.sku, i.name, i.category, i.on_hand, i.reorder_threshold, i.reorder_quantity,
       i.unit_cost_cents, i.daily_usage, i.vendor_id, v.name, v.email
from inventory i join vendors v on v.id = i.vendor_id`

// QueryExecutor answers inventory questions. Reading is free to replay, which is exactly why it
// contrasts well with the email step in the demo.
func (s *Store) QueryExecutor() executor.Executor { return &queryExecutor{store: s} }

type queryExecutor struct{ store *Store }

func (q *queryExecutor) Close() error { return nil }

func (q *queryExecutor) Execute(ctx context.Context, req executor.Request) (json.RawMessage, error) {
	if err := pace(ctx, q.store.StepDelay); err != nil {
		return nil, err
	}
	var in QueryInput
	if err := json.Unmarshal(req.Input, &in); err != nil {
		return nil, fmt.Errorf("decode query input: %w", err)
	}

	var (
		sql  string
		args []any
	)
	switch in.Query {
	case "low_stock":
		sql = inventorySelect + " where i.on_hand < i.reorder_threshold order by i.on_hand::float / i.reorder_threshold"
	case "inventory":
		sql = inventorySelect + " order by i.name"
	case "category":
		if in.Category == "" {
			return nil, errors.New("category query needs a category")
		}
		sql = inventorySelect + " where i.category = $1 order by i.name"
		args = append(args, in.Category)
	default:
		return nil, fmt.Errorf("unknown query %q (have low_stock, inventory, category)", in.Query)
	}

	rows, err := q.store.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("inventory query: %w", err)
	}
	defer rows.Close()

	out := QueryOutput{Query: in.Query, Rows: []InventoryRow{}}
	for rows.Next() {
		var r InventoryRow
		if err := rows.Scan(&r.SKU, &r.Name, &r.Category, &r.OnHand, &r.ReorderThreshold,
			&r.ReorderQuantity, &r.UnitCostCents, &r.DailyUsage, &r.VendorID, &r.VendorName, &r.VendorEmail); err != nil {
			return nil, fmt.Errorf("scan inventory: %w", err)
		}
		out.Rows = append(out.Rows, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read inventory: %w", err)
	}
	out.Count = len(out.Rows)
	return json.Marshal(out)
}

// LineItem is one product on a purchase order.
type LineItem struct {
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Quantity  int    `json:"quantity"`
	UnitCents int    `json:"unit_cost_cents"`
}

type EmailInput struct {
	VendorID  string     `json:"vendor_id"`
	Subject   string     `json:"subject"`
	Body      string     `json:"body"`
	LineItems []LineItem `json:"line_items"`
}

type EmailOutput struct {
	PurchaseOrderID int64  `json:"purchase_order_id"`
	VendorEmail     string `json:"vendor_email"`
	TotalCents      int    `json:"total_cents"`

	// Duplicate reports that this exact step had already been sent and was not sent again. It is
	// the visible half of the idempotency story: on a clean run it is false, and on a run killed
	// mid-send it is true for exactly the step that was in flight.
	Duplicate bool `json:"duplicate"`
}

// EmailExecutor sends a purchase order to a vendor, the workload's one real side effect.
func (s *Store) EmailExecutor() executor.Executor { return &emailExecutor{store: s} }

type emailExecutor struct{ store *Store }

func (e *emailExecutor) Close() error { return nil }

func (e *emailExecutor) Execute(ctx context.Context, req executor.Request) (json.RawMessage, error) {
	var in EmailInput
	if err := json.Unmarshal(req.Input, &in); err != nil {
		return nil, fmt.Errorf("decode email input: %w", err)
	}
	if in.VendorID == "" || len(in.LineItems) == 0 {
		return nil, errors.New("purchase order needs a vendor and at least one line item")
	}

	var vendorEmail string
	err := e.store.pool.QueryRow(ctx, "select email from vendors where id = $1", in.VendorID).Scan(&vendorEmail)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("no such vendor %q", in.VendorID)
	}
	if err != nil {
		return nil, fmt.Errorf("look up vendor: %w", err)
	}

	total := 0
	for _, item := range in.LineItems {
		total += item.Quantity * item.UnitCents
	}
	items, err := json.Marshal(in.LineItems)
	if err != nil {
		return nil, fmt.Errorf("encode line items: %w", err)
	}

	// The insert is the send, and the unique index on idempotency_key is what makes a retried
	// send harmless. A step retried after a crash conflicts here and orders nothing twice.
	var id int64
	err = e.store.pool.QueryRow(ctx, `
        insert into purchase_orders
            (idempotency_key, run_id, vendor_id, vendor_email, subject, body, line_items, total_cents)
        values ($1, $2, $3, $4, $5, $6, $7, $8)
        on conflict (idempotency_key) do nothing
        returning id`,
		req.IdempotencyKey, req.RunID, in.VendorID, vendorEmail, in.Subject, in.Body, items, total,
	).Scan(&id)

	// The delay sits after the insert rather than before it, and that placement is the demo.
	//
	// A send is not instant: the order is placed and then something waits on confirmation. That
	// wait is the dangerous window -- the side effect has already happened but the control plane
	// has not recorded it yet, so a crash here leaves a pending step and a real order in the world.
	// Pausing before the insert instead would make the window unreachable by hand and the
	// idempotency guarantee unobservable, which would demo nothing.
	if perr := pace(ctx, e.store.StepDelay); perr != nil {
		return nil, perr
	}

	if errors.Is(err, pgx.ErrNoRows) {
		if err := e.store.pool.QueryRow(ctx,
			"select id from purchase_orders where idempotency_key = $1", req.IdempotencyKey).Scan(&id); err != nil {
			return nil, fmt.Errorf("look up already-sent order: %w", err)
		}
		return json.Marshal(EmailOutput{
			PurchaseOrderID: id, VendorEmail: vendorEmail, TotalCents: total, Duplicate: true,
		})
	}
	if err != nil {
		return nil, fmt.Errorf("send purchase order: %w", err)
	}
	return json.Marshal(EmailOutput{PurchaseOrderID: id, VendorEmail: vendorEmail, TotalCents: total})
}

// pace waits out the demo delay, and gives up if the run is cancelled while waiting.
func pace(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Order is a purchase order as the outbox page shows it.
type Order struct {
	ID          int64      `json:"id"`
	RunID       string     `json:"run_id"`
	VendorID    string     `json:"vendor_id"`
	VendorEmail string     `json:"vendor_email"`
	Subject     string     `json:"subject"`
	Body        string     `json:"body"`
	LineItems   []LineItem `json:"line_items"`
	TotalCents  int        `json:"total_cents"`
	SentAt      time.Time  `json:"sent_at"`
}

// Inventory returns every product, for the stock panel.
func (s *Store) Inventory(ctx context.Context) ([]InventoryRow, error) {
	rows, err := s.pool.Query(ctx, inventorySelect+" order by i.category, i.name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []InventoryRow{}
	for rows.Next() {
		var r InventoryRow
		if err := rows.Scan(&r.SKU, &r.Name, &r.Category, &r.OnHand, &r.ReorderThreshold,
			&r.ReorderQuantity, &r.UnitCostCents, &r.DailyUsage, &r.VendorID, &r.VendorName, &r.VendorEmail); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Orders returns what has actually been sent. This is the panel that makes the idempotency claim
// checkable by eye: kill the control plane mid-send, resume, and this list still has one row.
func (s *Store) Orders(ctx context.Context) ([]Order, error) {
	rows, err := s.pool.Query(ctx, `
        select id, run_id, vendor_id, vendor_email, subject, body, line_items, total_cents, sent_at
        from purchase_orders order by id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []Order{}
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.ID, &o.RunID, &o.VendorID, &o.VendorEmail, &o.Subject, &o.Body,
			&o.LineItems, &o.TotalCents, &o.SentAt); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
