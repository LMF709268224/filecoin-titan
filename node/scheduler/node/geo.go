package node

import (
	"strings"
	"sync"

	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/region"
)

const (
	unknown = "unknown"
)

// GeoMgr node geo info manager
type GeoMgr struct {
	edgeGeoMap map[string]map[string]map[string]map[string][]*types.NodeInfo
	edgeLock   sync.Mutex

	candidateGeoMap map[string]map[string]map[string]map[string][]*types.NodeInfo
	candidateLock   sync.Mutex
}

func newGeoMgr() *GeoMgr {
	return &GeoMgr{
		edgeGeoMap:      make(map[string]map[string]map[string]map[string][]*types.NodeInfo),
		candidateGeoMap: make(map[string]map[string]map[string]map[string][]*types.NodeInfo),
	}
}

// AddNodeGeo add node to map
func (m *GeoMgr) AddNodeGeo(nodeInfo *types.NodeInfo, areaID string) {
	continent, country, province, city := region.DecodeAreaID(areaID)
	if nodeInfo.Type == types.NodeEdge {
		m.AddEdgeNode(continent, country, province, city, nodeInfo)
	} else if nodeInfo.Type == types.NodeCandidate {
		m.AddCandidateNode(continent, country, province, city, nodeInfo)
	}
}

// RemoveNodeGeo remove node from map
func (m *GeoMgr) RemoveNodeGeo(nodeID string, nodeType types.NodeType, areaID string) {
	continent, country, province, city := region.DecodeAreaID(areaID)
	if nodeType == types.NodeEdge {
		m.RemoveEdgeNode(continent, country, province, city, nodeID)
	} else if nodeType == types.NodeCandidate {
		m.RemoveCandidateNode(continent, country, province, city, nodeID)
	}
}

// FindNodesFromGeo find node from map
func (m *GeoMgr) FindNodesFromGeo(continent, country, province, city string, nodeType types.NodeType) []*types.NodeInfo {
	if nodeType == types.NodeEdge {
		return m.FindEdgeNodes(continent, country, province, city)
	} else if nodeType == types.NodeCandidate {
		return m.FindCandidateNodes(continent, country, province, city)
	}

	return nil
}

// GetGeoKey get node geo key
func (m *GeoMgr) GetGeoKey(continent, country, province string) map[string]int {
	return m.GetEdgeGeoKey(continent, country, province)
}

// AddEdgeNode add edge to map
func (m *GeoMgr) AddEdgeNode(continent, country, province, city string, nodeInfo *types.NodeInfo) {
	m.edgeLock.Lock()
	defer m.edgeLock.Unlock()

	if m.edgeGeoMap[continent] == nil {
		m.edgeGeoMap[continent] = make(map[string]map[string]map[string][]*types.NodeInfo)
	}
	if m.edgeGeoMap[continent][country] == nil {
		m.edgeGeoMap[continent][country] = make(map[string]map[string][]*types.NodeInfo)
	}
	if m.edgeGeoMap[continent][country][province] == nil {
		m.edgeGeoMap[continent][country][province] = make(map[string][]*types.NodeInfo)
	}
	m.edgeGeoMap[continent][country][province][city] = append(m.edgeGeoMap[continent][country][province][city], nodeInfo)
}

// RemoveEdgeNode remove edge from map
func (m *GeoMgr) RemoveEdgeNode(continent, country, province, city, nodeID string) {
	m.edgeLock.Lock()
	defer m.edgeLock.Unlock()

	if m.edgeGeoMap[continent] == nil || m.edgeGeoMap[continent][country] == nil || m.edgeGeoMap[continent][country][province] == nil {
		return
	}

	nodes := m.edgeGeoMap[continent][country][province][city]
	for i, nodeInfo := range nodes {
		if nodeInfo.NodeID == nodeID {
			m.edgeGeoMap[continent][country][province][city] = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}
}

// FindEdgeNodes find edge from map
func (m *GeoMgr) FindEdgeNodes(continent, country, province, city string) []*types.NodeInfo {
	m.edgeLock.Lock()
	defer m.edgeLock.Unlock()

	continent = strings.ToLower(continent)
	country = strings.ToLower(country)
	province = strings.ToLower(province)
	city = strings.ToLower(city)

	if continent != "" && country != "" && province != "" && city != "" {
		if m.edgeGeoMap[continent] != nil && m.edgeGeoMap[continent][country] != nil && m.edgeGeoMap[continent][country][province] != nil {
			return m.edgeGeoMap[continent][country][province][city]
		}
	} else if continent != "" && country != "" && province != "" {
		if m.edgeGeoMap[continent] != nil && m.edgeGeoMap[continent][country] != nil {
			var result []*types.NodeInfo
			for _, nodes := range m.edgeGeoMap[continent][country][province] {
				result = append(result, nodes...)
			}
			return result
		}
	} else if continent != "" && country != "" {
		if m.edgeGeoMap[continent] != nil {
			var result []*types.NodeInfo
			for _, provinces := range m.edgeGeoMap[continent][country] {
				for _, nodes := range provinces {
					result = append(result, nodes...)
				}
			}
			return result
		}
	} else if continent != "" {
		if countries, ok := m.edgeGeoMap[continent]; ok {
			var result []*types.NodeInfo
			for _, provinces := range countries {
				for _, cities := range provinces {
					for _, nodes := range cities {
						result = append(result, nodes...)
					}
				}
			}
			return result
		}
	}

	return nil
}

// GetEdgeGeoKey get edge geo key
func (m *GeoMgr) GetEdgeGeoKey(continent, country, province string) map[string]int {
	m.edgeLock.Lock()
	defer m.edgeLock.Unlock()

	continent = strings.ToLower(continent)
	country = strings.ToLower(country)
	province = strings.ToLower(province)

	result := make(map[string]int)
	if continent != "" && country != "" && province != "" {
		if countries, ok := m.edgeGeoMap[continent]; ok {
			if provinces, ok := countries[country]; ok {
				if cities, ok := provinces[province]; ok {
					for city, list := range cities {
						result[city] = len(list)
					}
				}
			}
		}
		return result
	} else if continent != "" && country != "" {
		if countries, ok := m.edgeGeoMap[continent]; ok {
			if provinces, ok := countries[country]; ok {
				for prov, cities := range provinces {
					for _, list := range cities {
						result[prov] += len(list)
					}
				}
			}
		}
		return result
	} else if continent != "" {
		if countries, ok := m.edgeGeoMap[continent]; ok {
			for count, provinces := range countries {
				for _, cities := range provinces {
					for _, list := range cities {
						result[count] += len(list)
					}
				}
			}
		}
		return result
	}

	for cont, countries := range m.edgeGeoMap {
		for _, provinces := range countries {
			for _, cities := range provinces {
				for _, list := range cities {
					result[cont] += len(list)
				}
			}
		}
	}

	return result
}

// AddCandidateNode add candidate to map
func (m *GeoMgr) AddCandidateNode(continent, country, province, city string, nodeInfo *types.NodeInfo) {
	m.candidateLock.Lock()
	defer m.candidateLock.Unlock()

	if m.candidateGeoMap[continent] == nil {
		m.candidateGeoMap[continent] = make(map[string]map[string]map[string][]*types.NodeInfo)
	}
	if m.candidateGeoMap[continent][country] == nil {
		m.candidateGeoMap[continent][country] = make(map[string]map[string][]*types.NodeInfo)
	}
	if m.candidateGeoMap[continent][country][province] == nil {
		m.candidateGeoMap[continent][country][province] = make(map[string][]*types.NodeInfo)
	}
	m.candidateGeoMap[continent][country][province][city] = append(m.candidateGeoMap[continent][country][province][city], nodeInfo)
}

// RemoveCandidateNode remove candidate from map
func (m *GeoMgr) RemoveCandidateNode(continent, country, province, city, nodeID string) {
	m.candidateLock.Lock()
	defer m.candidateLock.Unlock()

	if m.candidateGeoMap[continent] == nil || m.candidateGeoMap[continent][country] == nil || m.candidateGeoMap[continent][country][province] == nil {
		return
	}

	nodes := m.candidateGeoMap[continent][country][province][city]
	for i, nodeInfo := range nodes {
		if nodeInfo.NodeID == nodeID {
			m.candidateGeoMap[continent][country][province][city] = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}
}

// FindCandidateNodes find candidate from map
func (m *GeoMgr) FindCandidateNodes(continent, country, province, city string) []*types.NodeInfo {
	m.candidateLock.Lock()
	defer m.candidateLock.Unlock()

	continent = strings.ToLower(continent)
	country = strings.ToLower(country)
	province = strings.ToLower(province)
	city = strings.ToLower(city)

	if continent != "" && country != "" && province != "" && city != "" {
		if countries, ok := m.candidateGeoMap[continent]; ok {
			if provinces, ok := countries[country]; ok {
				if cities, ok := provinces[province]; ok {
					return cities[city]
				}
			}
		}
	} else if continent != "" && country != "" && province != "" {
		if countries, ok := m.candidateGeoMap[continent]; ok {
			if provinces, ok := countries[country]; ok {
				if cities, ok := provinces[province]; ok {
					var result []*types.NodeInfo
					for _, nodes := range cities {
						result = append(result, nodes...)
					}
					return result
				}
			}
		}
	} else if continent != "" && country != "" {
		if countries, ok := m.candidateGeoMap[continent]; ok {
			if provinces, ok := countries[country]; ok {
				var result []*types.NodeInfo
				for _, cities := range provinces {
					for _, nodes := range cities {
						result = append(result, nodes...)
					}
				}
				return result
			}
		}
	} else if continent != "" {
		if countries, ok := m.candidateGeoMap[continent]; ok {
			var result []*types.NodeInfo
			for _, provinces := range countries {
				for _, cities := range provinces {
					for _, nodes := range cities {
						result = append(result, nodes...)
					}
				}
			}
			return result
		}
	}

	return nil
}
