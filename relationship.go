package assets

import (
	"fmt"

	guuid "github.com/google/uuid"
)

type Relationship struct {
	ID       string
	Label    string
	From     string
	FromName string
	To       string
	ToName   string
}

func newRelationship(from, to Node, label string) Relationship {
	return Relationship{
		ID:       guuid.New().String(),
		Label:    label,
		To:       to.id,
		ToName:   to.name,
		From:     from.id,
		FromName: from.name,
	}
}

func (r Relationship) String() string {
	return fmt.Sprintf("{rel:%s-%s-%s}", r.FromName, r.Label, r.ToName)
}
