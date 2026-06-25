package broker

import "testing"

// TestRouteRoundRobinAndDrop checks the two load-bearing behaviours of the
// broker: round-robin across clients in a group, and drop when a client's
// bounded queue is full (backpressure).
func TestRouteRoundRobinAndDrop(t *testing.T) {
	s := NewServer(2)
	c1 := &client{out: make(chan []byte, 2), done: make(chan struct{})}
	c2 := &client{out: make(chan []byte, 2), done: make(chan struct{})}
	s.subs["sub"] = map[string]*grp{"grp": {clients: []*client{c1, c2}}}

	// 4 messages -> 2 each, round-robin, no drops (queue depth 2).
	for i := 0; i < 4; i++ {
		s.route("sub", []byte("x"))
	}
	if len(c1.out) != 2 || len(c2.out) != 2 {
		t.Fatalf("round-robin uneven: c1=%d c2=%d", len(c1.out), len(c2.out))
	}
	if c1.dropped.Load() != 0 || c2.dropped.Load() != 0 {
		t.Fatalf("unexpected drops: c1=%d c2=%d", c1.dropped.Load(), c2.dropped.Load())
	}

	// 2 more -> both queues full -> one drop each.
	s.route("sub", []byte("x"))
	s.route("sub", []byte("x"))
	if c1.dropped.Load() != 1 || c2.dropped.Load() != 1 {
		t.Fatalf("expected 1 drop each, got c1=%d c2=%d", c1.dropped.Load(), c2.dropped.Load())
	}
}
