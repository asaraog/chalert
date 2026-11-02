package rule

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/garbett1/chalert/config"
	"github.com/garbett1/chalert/datasource"
)

// tsQuerier records the timestamp each query is evaluated at.
type tsQuerier struct {
	mu  sync.Mutex
	ts  []time.Time
	got chan struct{}
}

func (q *tsQuerier) Query(_ context.Context, _ string, ts time.Time) (datasource.Result, error) {
	q.mu.Lock()
	q.ts = append(q.ts, ts)
	q.mu.Unlock()
	select {
	case q.got <- struct{}{}:
	default:
	}
	return datasource.Result{}, nil
}

func (q *tsQuerier) QueryRange(ctx context.Context, expr string, _, ts time.Time) (datasource.Result, error) {
	return q.Query(ctx, expr, ts)
}

func (q *tsQuerier) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ts)
}

// waitForQueries returns the latest query's timestamp once more than n
// queries have run, failing the test after a few seconds.
func (q *tsQuerier) waitForQueries(t *testing.T, n int) time.Time {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for q.count() <= n {
		select {
		case <-q.got:
		case <-deadline:
			t.Fatal("no query ran")
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.ts[len(q.ts)-1]
}

func delayedGroup(q datasource.Querier, delay time.Duration) *Group {
	cfg := config.Group{Name: "g", Interval: config.Duration{D: 20 * time.Millisecond}, Rules: []config.Rule{{Alert: "None", Expr: exprNoRows, ID: 1}}}
	if delay > 0 {
		cfg.EvalDelay = &config.Duration{D: delay}
	}
	return NewGroup(cfg, &fakeQuerierBuilder{q: q}, GroupOptions{DefaultInterval: time.Minute})
}

// assertDelay checks that a query ran about delay behind the clock.
func assertDelay(t *testing.T, ts time.Time, delay time.Duration) {
	t.Helper()
	if d := time.Since(ts) - delay; d < -5*time.Second || d > 5*time.Second {
		t.Errorf("query evaluated at %s, %s ago; want about %s ago", ts, time.Since(ts), delay)
	}
}

// A reload that changes a group's eval_delay changes the timestamp its rules
// are evaluated at. The group's ID does not change with its delay, so the
// running group is updated in place rather than replaced.
func TestUpdateAppliesAChangedEvalDelay(t *testing.T) {
	q := &tsQuerier{got: make(chan struct{}, 1)}
	g := delayedGroup(q, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { g.Start(ctx, nil, nil, nil); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	assertDelay(t, q.waitForQueries(t, 0), 30*time.Second)

	for _, delay := range []time.Duration{2 * time.Minute, 0} {
		ng := delayedGroup(q, delay)
		if ng.ID() != g.ID() {
			t.Fatalf("group ID changed with eval_delay: %d != %d", ng.ID(), g.ID())
		}
		g.UpdateWith(ng)
		// UpdateWith returns once the loop has taken the update, so every
		// later query runs with the new delay.
		assertDelay(t, q.waitForQueries(t, q.count()), delay)
	}
}
