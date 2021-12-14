package assets

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrNotFound = errors.New("could not find an element matching the request")
)

// FilterNodes s used to provide custom filters to when listing nodes
type FilterNodes func(node Node) bool

// FilterNodesByLabel filters all nodes with a given label
func FilterNodesByLabel(labels ...string) FilterNodes {
	return func(node Node) bool {
		for _, label := range labels {
			if node.GetLabel() == label {
				return true
			}
		}
		return false
	}
}

// FilterNodesByName filters all the nodes with the given names
func FilterNodesByName(names ...string) FilterNodes {
	return func(node Node) bool {
		for _, name := range names {
			if node.GetName() == name {
				return true
			}
		}
		return false
	}
}

// FilterRelationship is used to provide custom filters to when listing relationships
type FilterRelationship func(rel Relationship) bool

// FilterRelByLabel filters all relationships with a given label
func FilterRelByLabel(label string) FilterRelationship {
	return func(rel Relationship) bool {
		return rel.Label == label
	}
}

// FilterRelByTo filters all relationships which point to a node with the given ID
func FilterRelByTo(toID string) FilterRelationship {
	return func(rel Relationship) bool {
		return rel.To == toID
	}
}

// FilterRelByFrom filters all relationships which point from a node with the given ID
func FilterRelByFrom(fromID string) FilterRelationship {
	return func(rel Relationship) bool {
		return rel.From == fromID
	}
}

// New creates a new graph instance
func NewGraph() *Graph {
	return &Graph{
		nodes:         map[string]Node{},
		relationships: map[string]Relationship{},
	}
}

// Graph represents a collection of different nodes of the same type
type Graph struct {
	sync.RWMutex
	nodes         map[string]Node
	relationships map[string]Relationship
}

// InsertNode adds a new node to the graph
func (g *Graph) InsertNode(name, label string, body []byte) Node {
	g.Lock()
	defer g.Unlock()
	node := newNode(name, label, body)
	g.nodes[node.id] = node
	return node
}

// GetNodeByID returns the node that has the given ID
func (g *Graph) GetNodeByID(id string) (Node, error) {
	g.RLock()
	defer g.RUnlock()
	item, ok := g.nodes[id]
	if !ok {
		return Node{}, fmt.Errorf("%w; node with id '%s'", ErrNotFound, id)
	}
	return item, nil
}

// ListNodes returns a map of all the nodes that match all the where clauses provided.
func (g *Graph) ListNodes(where ...FilterNodes) []Node {
	g.RLock()
	defer g.RUnlock()
	matchingNodes := make([]Node, 0, len(g.nodes))
	for _, item := range g.nodes {
		matches := true
		for _, clause := range where {
			if ok := clause(item); !ok {
				matches = false
				break
			}
		}
		if matches {
			matchingNodes = append(matchingNodes, item)
		}
	}

	return matchingNodes
}

func (g *Graph) UpdateNode(nodeID string, body []byte) (Node, error) {
	g.Lock()
	defer g.Unlock()
	node, ok := g.nodes[nodeID]
	if !ok {
		return Node{}, fmt.Errorf("%w; node with id '%s'", ErrNotFound, nodeID)
	}
	node.Body = body
	g.nodes[node.id] = node
	return node, nil
}

func (g *Graph) DeleteNode(nodeID string) error {
	g.Lock()
	defer g.Unlock()
	node, ok := g.nodes[nodeID]
	if !ok {
		return fmt.Errorf("%w; node with id '%s'", ErrNotFound, nodeID)
	}
	delete(g.nodes, node.id)
	return nil
}

// AddRelationship is used to establish a directional relationship between the two items in the graph
func (g *Graph) AddRelationship(from, to Node, label string) (Relationship, error) {
	fromNode, err := g.GetNodeByID(from.GetID())
	if err != nil {
		return Relationship{}, fmt.Errorf("getNodeByID %s; %w", from.GetID(), err)
	}

	toNode, err := g.GetNodeByID(to.GetID())
	if err != nil {
		return Relationship{}, fmt.Errorf("getNodeByID %s; %w", to.GetID(), err)
	}
	g.Lock()
	defer g.Unlock()
	rel := newRelationship(fromNode, toNode, label)
	g.relationships[rel.ID] = rel

	return rel, nil
}

// GetRelationshipByID returns the relationship with the given ID
func (g *Graph) GetRelationshipByID(id string) (Relationship, error) {
	g.RLock()
	defer g.RUnlock()
	item, ok := g.relationships[id]
	if !ok {
		return Relationship{}, fmt.Errorf("%w; relationship with id '%s'", ErrNotFound, id)
	}
	return item, nil
}

// ListRelationships returns a list of all relationships with match the given filters. If no filters are provided returns all relationships
func (g *Graph) ListRelationships(filters ...FilterRelationship) []Relationship {
	g.RLock()
	defer g.RUnlock()
	matchingRelationships := make([]Relationship, 0, len(g.relationships))
	for _, item := range g.relationships {
		matches := true
		for _, clause := range filters {
			if ok := clause(item); !ok {
				matches = false
				break
			}
		}
		if matches {
			matchingRelationships = append(matchingRelationships, item)
		}
	}

	return matchingRelationships
}

// ListConnections returns all connection chains between a source node to a destination node by following relationships
func (g *Graph) ListConnections(from, to Node) []*ChainLink {
	return g.listConnections(from, to, map[string]struct{}{})
}

func (g *Graph) listConnections(from, to Node, visited map[string]struct{}) []*ChainLink {
	chains := []*ChainLink{}
	visited[from.id] = struct{}{}
	for _, rel := range g.ListRelationships(FilterRelByFrom(from.GetID())) {
		toCheck := copyMap(visited)
		// check if the relationship has already been visited. If it has, then go to the next one
		if _, ok := visited[rel.To]; ok {
			continue
		}
		toCheck[rel.To] = struct{}{}
		if rel.To == to.id {
			chains = append(chains, &ChainLink{node: from, rel: rel, next: &ChainLink{node: to}})
			continue
		}
		next, ok := g.nodes[rel.To]
		if !ok {
			continue
		}
		connections := g.listConnections(next, to, toCheck)
		for _, cons := range connections {
			chains = append(chains, &ChainLink{node: from, rel: rel, next: cons})
		}
	}
	return chains
}

func copyMap(m map[string]struct{}) map[string]struct{} {
	n := map[string]struct{}{}
	for k, v := range m {
		n[k] = v
	}
	return n
}

// ChainLink is used as a linked list item to show connections between different nodes
type ChainLink struct {
	node Node
	rel  Relationship
	next *ChainLink
}

func (c *ChainLink) String() string {
	var sb strings.Builder
	if c.node.GetID() != "" {
		sb.WriteString(c.node.String())
	}
	if c.rel.ID != "" {
		sb.WriteString(fmt.Sprintf("->%s", c.rel.String()))
	}
	if c.next != nil {
		sb.WriteString(fmt.Sprintf("->%s", c.next.String()))
	}
	return sb.String()
}
