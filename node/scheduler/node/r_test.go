package node

import (
	"fmt"
	"testing"
	"time"
)

func TestXxx(t *testing.T) {
	// 51,[51]
	// hour score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [51 51 51 51 51 51 51 51 51 51 51]
	// day score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 58 50 49 49 49 49 48 48 48 48 48 48 48 47 47 47 47 47 47 47 47 47 47 47 77 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51]
	// week score c_9b8df163-fc73-4364-949e-2c88da4ee249 : [60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 60 58 50 49 49 49 49 48 48 48 48 48 48 48 47 47 47 47 47 47 47 47 47 47 47 77 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51 51]

	curBandwidthUpScore := int64(51)
	historyHourBandwidthUpScore := []int64{51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51}
	historyDayBandwidthUpScore := []int64{60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 58, 50, 49, 49, 49, 49, 48, 48, 48, 48, 48, 48, 48, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 47, 77, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51}
	// historyWeekBandwidthUpScore := []int64{}

	bandwidthUpScore := calcScore(curBandwidthUpScore, historyHourBandwidthUpScore, historyDayBandwidthUpScore, historyDayBandwidthUpScore)
	t.Logf("info: %d", bandwidthUpScore)
}

const (
	interval = 10 * time.Second
)

type NodeInfo struct {
	ID int
}

type NodePair struct {
	Node1, Node2 *NodeInfo
}

func getNodes(count int) []*NodeInfo {
	nodes := make([]*NodeInfo, count)
	for i := range nodes {
		nodes[i] = &NodeInfo{ID: i}
	}

	if count%2 != 0 {
		nodes = append(nodes, &NodeInfo{ID: -1}) // Add a dummy node if odd
	}

	return nodes
}

func TestXxx2(t *testing.T) {
	count := 3
	nodes := getNodes(count)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	round := 0
	for {
		<-ticker.C

		rounds := len(nodes) - 1
		pairs := generatePairs(nodes, round)

		fmt.Printf("Round %d pairs:\n", round+1)
		for _, pair := range pairs {
			if pair.Node1.ID != -1 && pair.Node2.ID != -1 {
				fmt.Printf("Node %d <-> Node %d\n", pair.Node1.ID, pair.Node2.ID)
				// go testBandwidth(pair)
			}
		}

		round++
		if round >= rounds {
			fmt.Println("All pairings completed. Restarting from the beginning.")
			round = 0

			count++
			nodes = getNodes(count)
		}
	}
}

func generatePairs(nodes []*NodeInfo, round int) []NodePair {
	n := len(nodes)
	pairs := make([]NodePair, n/2)

	for i := 0; i < n/2; i++ {
		j := (round + i) % (n - 1)
		if i == 0 {
			pairs[i] = NodePair{nodes[j], nodes[n-1]}
		} else {
			pairs[i] = NodePair{nodes[j], nodes[(round-i+n-1)%(n-1)]}
		}
	}

	return pairs
}
