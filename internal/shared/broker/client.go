package broker

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
	"time"

	"zenity/internal/shared/event"
)

// frame is the wire format — one compact JSON object per line.
//
//	subscribe control: {"sub":true,"subject":"...","group":"..."}
//	publish / deliver: {"subject":"...","data":<event json>}
type frame struct {
	Sub     bool            `json:"sub,omitempty"`
	Subject string          `json:"subject,omitempty"`
	Group   string          `json:"group,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Client is a connection to the broker, used by both producers and consumers.
type Client struct {
	conn net.Conn

	wmu sync.Mutex // serializes writes to the connection
	w   *bufio.Writer

	mu      sync.Mutex                   // guards subs
	subs    map[string]func(event.Event) // subject -> handler (consumer side)
	started bool
}

// Connect dials the broker, e.g. "localhost:9000". It tolerates the broker not
// being up yet (k8s start order), retrying until it connects or times out.
func Connect(addr string) (*Client, error) {
	conn, err := dialWithRetry(addr, 30*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn: conn,
		w:    bufio.NewWriter(conn),
		subs: make(map[string]func(event.Event)),
	}, nil
}

// dialWithRetry retries the TCP dial with a 1s backoff until it succeeds or the
// timeout elapses, so a producer/consumer started before the broker just waits.
func dialWithRetry(addr string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err == nil {
			return conn, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		log.Printf("[broker] waiting for %s: %v", addr, err)
		time.Sleep(time.Second)
	}
}

func (c *Client) writeFrame(f frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.w.Write(b); err != nil {
		return err
	}
	if err := c.w.WriteByte('\n'); err != nil {
		return err
	}
	return c.w.Flush()
}

// Publish sends an event on a subject. Satisfies router.Publisher.
func (c *Client) Publish(subject string, e event.Event) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.writeFrame(frame{Subject: subject, Data: data})
}

// Subscribe joins (subject, group) and calls fn for each delivered event.
// All subscriptions on one client share a single read loop, routed by subject.
func (c *Client) Subscribe(subject, group string, fn func(event.Event)) error {
	c.mu.Lock()
	c.subs[subject] = fn
	start := !c.started
	c.started = true
	c.mu.Unlock()

	if err := c.writeFrame(frame{Sub: true, Subject: subject, Group: group}); err != nil {
		return err
	}
	if start {
		go c.readLoop()
	}
	return nil
}

func (c *Client) readLoop() {
	sc := bufio.NewScanner(c.conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var f frame
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			continue
		}
		c.mu.Lock()
		fn := c.subs[f.Subject]
		c.mu.Unlock()
		if fn == nil {
			continue
		}
		if e, err := event.Decode(f.Data); err == nil {
			fn(e)
		}
	}
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }
