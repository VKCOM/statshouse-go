package statshouse

import (
	"context"
	"net"
	"strings"
	"sync"

	"pgregory.net/rand"
)

type addressPool struct {
	mu    sync.Mutex
	addrs []string
	head  int
}

func newAddressPool(addrs []string) *addressPool {
	r := rand.New()
	r.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return &addressPool{
		addrs: addrs,
	}
}

func (p *addressPool) pick() (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.addrs)
	if n == 0 {
		return "", false
	}
	addr := p.addrs[p.head]
	p.head = (p.head + 1) % n
	return addr, true
}

func resolveDialTargets(ctx context.Context, network, rawAddr string) ([]string, error) {
	if rawAddr == "" {
		return []string{""}, nil
	}
	if network == "unixgram" {
		return []string{rawAddr}, nil
	}
	var expanded []string
	for _, ep := range strings.Split(rawAddr, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(ep))
		if err != nil {
			return []string{""}, err
		}
		if ip := net.ParseIP(host); ip != nil {
			expanded = append(expanded, net.JoinHostPort(host, port))
			continue
		}
		recs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return []string{""}, err
		}
		for _, ipa := range recs {
			expanded = append(expanded, net.JoinHostPort(ipa.String(), port))
		}
	}
	return expanded, nil
}
