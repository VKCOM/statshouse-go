package statshouse

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestResolveTargetBudgetFailureReleasesTemporaryState(t *testing.T) {
	b, _ := NewMemoryBudget(512)
	base := b.Snapshot().UsedBytes
	_, _, err := resolveDialTargetsBudget(context.Background(), "tcp", strings.Repeat("127.0.0.1:1,", 100)+"127.0.0.1:1", b)
	if !errors.Is(err, ErrMemoryBudgetExceeded) {
		t.Fatal(err)
	}
	if b.Snapshot().UsedBytes != base {
		t.Fatalf("resolution charge leaked: %+v", b.Snapshot())
	}
}
func TestResolveTargetsPreservesUDPOrder(t *testing.T) {
	targets, err := resolveDialTargets("udp", "first:1,second:2")
	if err != nil || len(targets) != 2 || targets[0] != "first:1" || targets[1] != "second:2" {
		t.Fatal(targets, err)
	}
}
func TestResolveTargetTemporaryCharge(t *testing.T) {
	b, _ := NewMemoryBudget(4096)
	base := b.Snapshot().UsedBytes
	targets, l, err := resolveDialTargetsBudget(context.Background(), "tcp", "127.0.0.1:1,127.0.0.2:2", b)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || b.Snapshot().UsedBytes <= base {
		t.Fatal("unaccounted targets")
	}
	targets = nil
	l.shrink(l.bytes.Load())
	if b.Snapshot().UsedBytes != base {
		t.Fatal("leaked")
	}
}
