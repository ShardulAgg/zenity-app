package broker

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

// Server is the broker: it accepts connections, routes published frames by
// subject, and load-balances across the clients in a group (round-robin).
type Server struct {
	depth int // per-client send-queue depth (backpressure bound)

	mu   sync.Mutex
	subs map[string]map[string]*grp // subject -> group -> grp
}

func NewServer(depth int) *Server {
	return &Server{depth: depth, subs: make(map[string]map[string]*grp)}
}

// grp is one (subject, group): the set of clients that share its load.
type grp struct {
	mu      sync.Mutex
	clients []*client
	next    int // round-robin cursor
}

type client struct {
	nc      net.Conn
	out     chan []byte // bounded send queue — the backpressure point
	done    chan struct{}
	dropped atomic.Int64
}

type reg struct {
	subject, group string
	cl             *client
}

// ListenAndServe blocks, accepting connections on addr (e.g. ":9000").
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("[broker] listening on %s", addr)
	for {
		nc, err := ln.Accept()
		if err != nil {
			return err
		}
		go s.handle(nc)
	}
}

func (s *Server) handle(nc net.Conn) {
	cl := &client{nc: nc, out: make(chan []byte, s.depth), done: make(chan struct{})}
	go cl.writeLoop()

	var joined []reg
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		var f frame
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			continue
		}
		if f.Sub {
			s.join(f.Subject, f.Group, cl)
			joined = append(joined, reg{f.Subject, f.Group, cl})
		} else {
			line := append([]byte(nil), sc.Bytes()...) // copy: scanner reuses its buffer
			s.route(f.Subject, line)
		}
	}

	for _, r := range joined {
		s.leave(r)
	}
	close(cl.done)
	nc.Close()
}

func (cl *client) writeLoop() {
	w := bufio.NewWriter(cl.nc)
	for {
		select {
		case line := <-cl.out:
			w.Write(line)
			w.WriteByte('\n')
			if err := w.Flush(); err != nil {
				return
			}
		case <-cl.done:
			return
		}
	}
}

func (s *Server) join(subject, group string, cl *client) {
	s.mu.Lock()
	if s.subs[subject] == nil {
		s.subs[subject] = make(map[string]*grp)
	}
	g := s.subs[subject][group]
	if g == nil {
		g = &grp{}
		s.subs[subject][group] = g
	}
	s.mu.Unlock()

	g.mu.Lock()
	g.clients = append(g.clients, cl)
	g.mu.Unlock()
	log.Printf("[broker] subscribe %s/%s", subject, group)
}

func (s *Server) leave(r reg) {
	s.mu.Lock()
	var g *grp
	if m := s.subs[r.subject]; m != nil {
		g = m[r.group]
	}
	s.mu.Unlock()
	if g == nil {
		return
	}
	g.mu.Lock()
	for i, c := range g.clients {
		if c == r.cl {
			g.clients = append(g.clients[:i], g.clients[i+1:]...)
			break
		}
	}
	g.mu.Unlock()
}

// route delivers one published line to ONE client per subscribed group
// (round-robin), dropping if that client's queue is full (backpressure).
func (s *Server) route(subject string, line []byte) {
	s.mu.Lock()
	m := s.subs[subject]
	gs := make([]*grp, 0, len(m))
	for _, g := range m {
		gs = append(gs, g)
	}
	s.mu.Unlock()

	for _, g := range gs {
		g.mu.Lock()
		if len(g.clients) == 0 {
			g.mu.Unlock()
			continue
		}
		cl := g.clients[g.next%len(g.clients)]
		g.next++
		g.mu.Unlock()

		select {
		case cl.out <- line:
		default:
			cl.dropped.Add(1) // queue full -> shed load
		}
	}
}
