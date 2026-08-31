// Command supplychain-agent is the demo agent: it checks stock, decides what needs reordering, and
// sends a purchase order to each vendor that supplies something short.
//
// Like every agent here it executes nothing itself. It asks for a query, is handed rows; asks for a
// purchase order to be sent, is told which one. On resume the control plane answers the earlier
// steps from Postgres, so the agent walks back through the same decisions without re-reading the
// database and without ordering twice.
//
// The reordering decision is deterministic by default so the demo costs nothing to run. Setting
// BULKHEAD_AGENT_USE_LLM=1 routes the same decision through a model as an llm step, which is
// checkpointed like any other step -- that is what keeps resume correct once a real model is in the
// loop, since a replayed run is handed the completion it saw the first time rather than sampling a
// new one.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/slmatthiesen/bulkhead/internal/protocol"
	"github.com/slmatthiesen/bulkhead/internal/workload/supplychain"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "supplychain-agent:", err)
		os.Exit(1)
	}
}

type session struct {
	in  *bufio.Scanner
	out *json.Encoder
	seq int
}

func run() error {
	s := &session{
		in:  bufio.NewScanner(os.Stdin),
		out: json.NewEncoder(os.Stdout),
	}
	s.in.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	// 1. What is running low?
	var stock supplychain.QueryOutput
	if err := s.step(supplychain.KindQuery, supplychain.QueryInput{Query: "low_stock"}, &stock); err != nil {
		return err
	}
	if stock.Count == 0 {
		return s.final("nothing below reorder threshold", true)
	}

	// 2. Decide. Grouping by vendor is what turns five short products into two purchase orders,
	// which is the part of the decision worth watching.
	byVendor := map[string][]supplychain.InventoryRow{}
	for _, row := range stock.Rows {
		byVendor[row.VendorID] = append(byVendor[row.VendorID], row)
	}
	vendors := make([]string, 0, len(byVendor))
	for id := range byVendor {
		vendors = append(vendors, id)
	}
	sort.Strings(vendors)

	if os.Getenv("BULKHEAD_AGENT_USE_LLM") != "" {
		if err := s.explain(stock); err != nil {
			return err
		}
	}

	// 3. One purchase order per vendor, followed by a Slack update so purchasing knows without
	// watching the console.
	sent := 0
	for _, vendorID := range vendors {
		rows := byVendor[vendorID]
		po := purchaseOrder(vendorID, rows)

		var approval supplychain.ApprovalOutput
		approvalIn := supplychain.ApprovalInput{VendorID: vendorID, Subject: po.Subject, TotalCents: orderTotal(rows)}
		if err := s.step(supplychain.KindApprove, approvalIn, &approval); err != nil {
			return err
		}

		var out supplychain.EmailOutput
		if err := s.step(supplychain.KindEmail, po, &out); err != nil {
			return err
		}
		sent++

		var notice supplychain.SlackOutput
		slackIn := supplychain.SlackInput{
			PurchaseOrderID: out.PurchaseOrderID,
			Channel:         "#purchasing",
			Message:         fmt.Sprintf("Purchase order sent to %s — $%.2f", out.VendorEmail, float64(out.TotalCents)/100),
		}
		if err := s.step(supplychain.KindSlack, slackIn, &notice); err != nil {
			return err
		}
	}

	return s.final(fmt.Sprintf("%d short products, %d purchase orders sent", stock.Count, sent), true)
}

// explain asks a model to justify the reorder in a sentence. The decision itself stays
// deterministic -- what the model produces is the wording on the purchase order, not the arithmetic
// of what to order, because a demo that lets a model invent quantities is demoing something else.
func (s *session) explain(stock supplychain.QueryOutput) error {
	var lines []string
	for _, r := range stock.Rows {
		line := fmt.Sprintf("%s: %d on hand, reorder below %d", r.Name, r.OnHand, r.ReorderThreshold)
		if r.DailyUsage > 0 {
			line += fmt.Sprintf(", using %d/day (%.1f days left)", r.DailyUsage, float64(r.OnHand)/float64(r.DailyUsage))
		}
		lines = append(lines, line)
	}
	// The operator's own words when there are any. A run that answers the question someone asked
	// reads as an agent; the same run answering a hardcoded prompt reads as a scheduled job that
	// happens to call a model.
	ask := os.Getenv("BULKHEAD_QUESTION")
	if strings.TrimSpace(ask) == "" {
		ask = "Summarise the situation for the purchasing team."
	}
	input := map[string]any{
		"system": "You are a warehouse operations assistant. Answer the question in two sentences at most, no preamble. Ground every claim in the stock figures you are given.",
		"messages": []map[string]string{{
			"role":    "user",
			"content": "Current stock below reorder threshold:\n" + strings.Join(lines, "\n") + "\n\n" + ask,
		}},
		// Generous, because the models this runs against reason before answering and a tight
		// budget is spent entirely on reasoning -- the request succeeds and the text comes back
		// empty, which looks like a bug and is not one.
		"max_tokens": 2000,
	}
	var out struct {
		Text string `json:"text"`
	}
	return s.step("llm", input, &out)
}

func purchaseOrder(vendorID string, rows []supplychain.InventoryRow) supplychain.EmailInput {
	items := make([]supplychain.LineItem, 0, len(rows))
	var body strings.Builder
	body.WriteString("Please supply the following at your earliest convenience:\n\n")
	for _, r := range rows {
		items = append(items, supplychain.LineItem{
			SKU: r.SKU, Name: r.Name, Quantity: r.ReorderQuantity, UnitCents: r.UnitCostCents,
		})
		fmt.Fprintf(&body, "  %-22s %s  x%d\n", r.SKU, r.Name, r.ReorderQuantity)
	}
	fmt.Fprintf(&body, "\nCurrent stock is below our reorder threshold on %d item(s).\n", len(rows))

	return supplychain.EmailInput{
		VendorID:  vendorID,
		Subject:   fmt.Sprintf("Purchase order: %d item(s) below reorder threshold", len(rows)),
		Body:      body.String(),
		LineItems: items,
	}
}

func orderTotal(rows []supplychain.InventoryRow) int {
	total := 0
	for _, r := range rows {
		total += r.ReorderQuantity * r.UnitCostCents
	}
	return total
}

// step asks the control plane to execute one step and decodes the result into out.
func (s *session) step(kind string, input, out any) error {
	s.seq++
	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode %s input: %w", kind, err)
	}
	if err := s.out.Encode(protocol.AgentMessage{
		Type:  protocol.KindStep,
		Seq:   s.seq,
		Step:  kind,
		Input: payload,
	}); err != nil {
		return fmt.Errorf("write step %d: %w", s.seq, err)
	}

	if !s.in.Scan() {
		if err := s.in.Err(); err != nil {
			return fmt.Errorf("read result %d: %w", s.seq, err)
		}
		return fmt.Errorf("control plane closed the stream before step %d", s.seq)
	}
	var res protocol.StepResult
	if err := json.Unmarshal(s.in.Bytes(), &res); err != nil {
		return fmt.Errorf("decode result %d: %w", s.seq, err)
	}
	if res.Error != "" {
		return fmt.Errorf("step %d (%s) failed: %s", s.seq, kind, res.Error)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(res.Output, out); err != nil {
		return fmt.Errorf("decode %s output: %w", kind, err)
	}
	return nil
}

func (s *session) final(summary string, ok bool) error {
	return s.out.Encode(protocol.AgentMessage{
		Type:        protocol.KindFinal,
		Patch:       summary,
		TestsPassed: ok,
	})
}
