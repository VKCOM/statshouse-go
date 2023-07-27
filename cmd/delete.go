package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/vkcom/statshouse-go"
)

func main() {
	statshouse.Configure(func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args)
	}, statshouse.DefaultStatsHouseAddr, "dev")
	statshouse.Metric("test", statshouse.Tags{1: "a"}).Count(1)
	statshouse.Metric("test", statshouse.Tags{1: "b"}).Count(1)
	statshouse.Metric("test", statshouse.Tags{1: strings.Repeat("a", 5000)}).Count(1)
	time.Sleep(2 * time.Second)
	for {
		statshouse.Metric("test", statshouse.Tags{1: "b"}).Count(1)
		statshouse.Metric("test", statshouse.Tags{1: strings.Repeat("a", 5000)}).Count(1)
		time.Sleep(2 * time.Second)
	}
}

/*
	success send 48
	set len inner 48
[statshouse] metric ["abc"] payload too big to fit into packet, discardingsuccess send 120
[statshouse] failed to send data to statshouse: [write udp [::1]:50702->[::1]:12345: write: connection refused]


*/
