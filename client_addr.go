package statshouse

import (
	"context"
	"net"
	"strings"

	"pgregory.net/rand"
)

type addressPool struct {
	addrs []string
	head  int
}

func newAddressPools(addrs []string) (primary *addressPool, secondary *addressPool) {
	mid := (len(addrs) + 1) / 2 // primary gets the extra element
	primary = &addressPool{addrs: addrs[:mid]}
	if mid < len(addrs) {
		secondary = &addressPool{addrs: addrs[mid:]}
	}
	return primary, secondary
}

func (p *addressPool) pick() (string, bool) {
	n := len(p.addrs)
	if n == 0 {
		return "", false
	}
	addr := p.addrs[p.head]
	p.head = (p.head + 1) % n
	return addr, true
}

func resolveDialTargets(network, rawAddr string) ([]string, error) {
	if rawAddr == "" {
		return []string{""}, nil
	}
	if network == "unixgram" {
		return []string{rawAddr}, nil
	}
	var addrs []string
	for _, ep := range strings.Split(rawAddr, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(ep))
		if err != nil {
			return []string{""}, err
		}
		if ip := net.ParseIP(host); ip != nil {
			addrs = append(addrs, net.JoinHostPort(host, port))
			continue
		}
		ctx := context.Background()
		recs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return []string{""}, err
		}
		for _, ipa := range recs {
			addrs = append(addrs, net.JoinHostPort(ipa.String(), port))
		}
	}
	r := rand.New()
	r.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return addrs, nil
}
