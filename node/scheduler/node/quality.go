package node

import (
	"time"
)

func (m *Manager) getPairsNode() []string {
	nodes, _ := m.GetResourceCandidateNodes()

	nodeCount := len(nodes)
	if nodeCount%2 != 0 {
		nodes = append(nodes, "") // Add a dummy node if odd
	}

	return nodes
}

func (m *Manager) qualityCheckTimer() {
	ticker := time.NewTicker(qualityCheckTime)
	defer ticker.Stop()

	time.Sleep(5 * time.Minute)
	nodes := m.getPairsNode()

	round := 0
	for {
		<-ticker.C

		rounds := len(nodes) - 1
		pairs := m.generatePairs(nodes, round)

		log.Infof("qualityCheck -- Round %d pairs:\n", round+1)
		for _, pair := range pairs {
			if pair.Node1 != "" && pair.Node2 != "" {
				log.Infof("qualityCheck -- %s <-> %s\n", pair.Node1, pair.Node2)
				// go testBandwidth(pair)
			}
		}

		round++
		if round >= rounds {
			log.Infoln("qualityCheck -- All pairings completed. Restarting from the beginning.")
			round = 0

			nodes = m.getPairsNode()
		}
	}
}

func (m *Manager) generatePairs(nodes []string, round int) []pair {
	n := len(nodes)
	if n < 2 {
		return []pair{}
	}

	pairs := make([]pair, n/2)

	for i := 0; i < n/2; i++ {
		j := (round + i) % (n - 1)
		if i == 0 {
			pairs[i] = pair{nodes[j], nodes[n-1]}
		} else {
			pairs[i] = pair{nodes[j], nodes[(round-i+n-1)%(n-1)]}
		}
	}

	return pairs
}

type pair struct {
	Node1, Node2 string
}
