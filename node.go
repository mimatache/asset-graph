package assets

import (
	"fmt"

	guuid "github.com/google/uuid"
)

func newNode(name, label string, body []byte) Node {
	return Node{
		id:    guuid.New().String(),
		label: label,
		name:  name,
		Body:  body,
	}
}

// Node represents an item in the graph. It contains the ID of the element, the body and it's relationships to other items
type Node struct {
	id    string
	name  string
	label string
	Body  []byte
}

func (n Node) GetID() string {
	return n.id
}

func (n Node) GetName() string {
	return n.name
}

func (n Node) GetLabel() string {
	return n.label
}

func (n Node) String() string {
	return fmt.Sprintf("{Asset:%s}", n.name)
}
