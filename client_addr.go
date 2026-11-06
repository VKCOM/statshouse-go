package statshouse

import (
	"context"
	"net"
	"strings"
	"unsafe"

	"pgregory.net/rand"
)

type addressPool struct {
	addrs []string
	head  int
}

func newAddressPools(addrs []string) (primary addressPool, secondary addressPool) {
	mid := (len(addrs) + 1) / 2
	primary = addressPool{addrs: addrs[:mid]}
	if mid < len(addrs) {
		secondary = addressPool{addrs: addrs[mid:]}
	}
	return
}
func (p *addressPool) len() int { return len(p.addrs) }
func (p *addressPool) pick() (string, bool) {
	if p.len() == 0 {
		return "", false
	}
	s := p.addrs[p.head]
	p.head = (p.head + 1) % len(p.addrs)
	return s, true
}
func resolveDialTargets(network, raw string) ([]string, error) {
	return resolveDialTargetsContext(context.Background(), network, raw)
}
func resolveDialTargetsContext(ctx context.Context, network, raw string) ([]string, error) {
	targets, _, err := resolveDialTargetsBudget(ctx, network, raw, nil)
	return targets, err
}

type targetBuilder struct {
	targets []string
	lease   *memoryLease
}

func (b *targetBuilder) grow(n int64) bool {
	if b.lease == nil {
		return true
	}
	return b.lease.grow(n)
}
func (b *targetBuilder) shrink(n int64) {
	if b.lease != nil {
		b.lease.shrink(n)
	}
}
func (b *targetBuilder) append(host, port string, join bool) error {
	if len(b.targets) == cap(b.targets) {
		size := 2*cap(b.targets) + 1
		n := int64(size) * int64(unsafe.Sizeof(""))
		if !b.grow(n) {
			return ErrMemoryBudgetExceeded
		}
		a := make([]string, len(b.targets), size)
		copy(a, b.targets)
		old := int64(cap(b.targets)) * int64(unsafe.Sizeof(""))
		b.targets = a
		b.shrink(old)
	}
	n := int64(len(host))
	if join {
		n += int64(len(port)) + 3
	}
	if !b.grow(n) {
		return ErrMemoryBudgetExceeded
	}
	var s string
	if join {
		s = net.JoinHostPort(host, port)
	} else {
		s = strings.Clone(host)
	}
	b.shrink(n - int64(len(s)))
	b.targets = append(b.targets, s)
	return nil
}
func resolveDialTargetsBudget(ctx context.Context, network, raw string, budget *MemoryBudget) (targets []string, lease *memoryLease, err error) {
	builder := targetBuilder{}
	if budget != nil {
		base := int64(unsafe.Sizeof(memoryLease{}))
		if !budget.reserve(base) {
			return nil, nil, ErrMemoryBudgetExceeded
		}
		builder.lease = makeLease(budget, base)
	}
	defer func() {
		if err != nil && builder.lease != nil {
			builder.targets = nil
			builder.lease.shrink(builder.lease.bytes.Load())
		}
	}()
	if raw == "" || network == "unixgram" {
		err = builder.append(raw, "", false)
		return builder.targets, builder.lease, err
	}
	rest := raw
	for {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		ep, next, more := strings.Cut(rest, ",")
		rest = next
		if network == "udp" {
			err = builder.append(ep, "", false)
		} else {
			var host, port string
			host, port, err = net.SplitHostPort(strings.TrimSpace(ep))
			if err != nil {
				return nil, nil, err
			}
			if net.ParseIP(host) != nil {
				err = builder.append(host, port, true)
			} else {
				var recs []net.IPAddr
				recs, err = net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, nil, err
				}
				for _, ip := range recs {
					if err = builder.append(ip.String(), port, true); err != nil {
						return nil, nil, err
					}
				}
			}
		}
		if err != nil {
			return nil, nil, err
		}
		if !more {
			break
		}
	}
	if network == "udp" {
		return builder.targets, builder.lease, nil
	}
	n := int64(unsafe.Sizeof(rand.Rand{}))
	if !builder.grow(n) {
		return nil, nil, ErrMemoryBudgetExceeded
	}
	r := rand.New()
	r.Shuffle(len(builder.targets), func(i, j int) { builder.targets[i], builder.targets[j] = builder.targets[j], builder.targets[i] })
	builder.shrink(n)
	return builder.targets, builder.lease, nil
}
