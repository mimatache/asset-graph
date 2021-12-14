package assets_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/curious-kitten/assets"
)

const (
	dragonType = "dragon"
	puppyType  = "puppy"
)

type puppy struct {
	Power int    `json:"power"`
	Name  string `json:"name"`
}

var (
	bobita     = "Bobita"
	bobitaBody = []byte("{\"name\":\"Bobita\", \"power\":500}")
	azor       = "Azor"
	azorBody   = []byte("{\"name\":\"Azor\", \"power\":457}")
	smaug      = "Smaug"
	smaugBody  = []byte("{\"name\":\"Azor\", \"power\":457, \"canFly\":true}")
)

func Test_Graph_InsertNode(t *testing.T) {
	grf := assets.NewGraph()
	createdNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	node, err := grf.GetNodeByID(createdNode.GetID())
	assert.NoError(t, err)
	assert.Equal(t, bobitaBody, node.Body)
	assert.Equal(t, puppyType, node.GetLabel())
}

func Test_Graph_UpdateNode(t *testing.T) {
	grf :=assets.NewGraph()
	createdNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	node, err := grf.GetNodeByID(createdNode.GetID())
	assert.NoError(t, err)
	assert.Equal(t, bobitaBody, node.Body)
	assert.Equal(t, puppyType, node.GetLabel())

	node, err = grf.UpdateNode(node.GetID(), azorBody)
	assert.NoError(t, err)
	assert.Equal(t, azorBody, node.Body)

	node, err = grf.GetNodeByID(node.GetID())
	assert.NoError(t, err)
	assert.Equal(t, azorBody, node.Body)
}

func Test_Graph_UpdateNode_NoNode(t *testing.T) {
	grf :=assets.NewGraph()
	_, err := grf.UpdateNode("node.GetID()", azorBody)
	assert.Error(t, err)
}

func Test_Graph_DeleteNode(t *testing.T) {
	grf :=assets.NewGraph()
	createdNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	node, err := grf.GetNodeByID(createdNode.GetID())
	assert.NoError(t, err)
	assert.Equal(t, bobitaBody, node.Body)
	assert.Equal(t, puppyType, node.GetLabel())

	err = grf.DeleteNode(node.GetID())
	assert.NoError(t, err)
}

func Test_Graph_DeleteNode_NoNode(t *testing.T) {
	grf :=assets.NewGraph()
	err := grf.DeleteNode("node.GetID()")
	assert.Error(t, err)
}

func Test_Graph_AddConcurrently(t *testing.T) {
	concurrencyCount := 100
	types := make([]string, concurrencyCount)
	for i := 0; i < concurrencyCount; i++ {
		types[i] = fmt.Sprintf("type-%d", i)
	}
	var wg sync.WaitGroup
	wg.Add(concurrencyCount)
	grf :=assets.NewGraph()
	for i := 0; i < concurrencyCount; i++ {
		go func(i int) {
			defer wg.Done()
			grf.InsertNode(fmt.Sprintf("item-%d", i), puppyType, []byte{})
		}(i)
	}
	wg.Wait()
	nodes := grf.ListNodes()
	assert.Equal(t, concurrencyCount, len(nodes), "not all node types were added")
}

func Test_Graph_AddRelationshipConcurrently(t *testing.T) {
	concurrencyCount := 100
	types := make([]string, concurrencyCount)
	grf :=assets.NewGraph()
	createdNodeOne := grf.InsertNode(bobita, puppyType, bobitaBody)
	createdNodeTwo := grf.InsertNode(azor, puppyType, azorBody)
	for i := 0; i < concurrencyCount; i++ {
		types[i] = fmt.Sprintf("type-%d", i)
	}
	var wg sync.WaitGroup
	wg.Add(concurrencyCount)
	for i := 0; i < concurrencyCount; i++ {
		go func(i int) {
			defer wg.Done()
			_, err := grf.AddRelationship(createdNodeOne, createdNodeTwo, fmt.Sprintf("item-%d", i))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
	rels := grf.ListRelationships()
	assert.Equal(t, concurrencyCount, len(rels), "not all node types were added")
}
func Test_Graph_GetNodes_Missing(t *testing.T) {
	grf :=assets.NewGraph()
	_, err := grf.GetNodeByID("bobitaNodeID")
	assert.True(t, errors.Is(err, assets.ErrNotFound))
}

func Test_Graph_List(t *testing.T) {
	grf :=assets.NewGraph()
	grf.InsertNode(bobita, puppyType, bobitaBody)
	grf.InsertNode(azor, puppyType, azorBody)
	grf.InsertNode(smaug, dragonType, smaugBody)
	foundNodes := grf.ListNodes()
	assert.Equal(t, 3, len(foundNodes))
}

func Test_Graph_ListNodes_FilterByLabel(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	grf.InsertNode(smaug, dragonType, smaugBody)
	foundNodes := grf.ListNodes(assets.FilterNodesByLabel(puppyType))
	assert.Equal(t, 1, len(foundNodes))
	assert.Equal(t, bNode.GetID(), foundNodes[0].GetID())
}

func Test_Graph_ListNodes_FilterByName(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	grf.InsertNode(smaug, dragonType, smaugBody)
	foundNodes := grf.ListNodes(assets.FilterNodesByName(bobita))
	assert.Equal(t, 1, len(foundNodes))
	assert.Equal(t, bNode.GetID(), foundNodes[0].GetID())
}

func Test_Graph_ListNodes_Filter(t *testing.T) {
	grf :=assets.NewGraph()
	whereCond := func(body assets.Node) bool {
		pup := puppy{}
		if err := json.Unmarshal(body.Body, &pup); err != nil {
			return false
		}
		return pup.Power > 499
	}
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	grf.InsertNode(azor, puppyType, azorBody)
	foundNodes := grf.ListNodes(whereCond)
	assert.Equal(t, 1, len(foundNodes))
	assert.Equal(t, bNode.GetID(), foundNodes[0].GetID())
}

func Test_Graph_AddRelationship(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	rel1, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	rel2, err := grf.AddRelationship(bNode, aNode, "competitors")
	assert.NoError(t, err)
	bNode, err = grf.GetNodeByID(bNode.GetID())
	assert.NoError(t, err)
	assert.Equal(t, 2, len(grf.ListRelationships()))
	assert.Contains(t, grf.ListRelationships(), rel1)
	assert.Contains(t, grf.ListRelationships(), rel2)
}

func Test_Graph_AddRelationship_NoFrom(t *testing.T) {
	grf :=assets.NewGraph()
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	node := assets.Node{}
	_, err := grf.AddRelationship(node, aNode, "friends")
	assert.Error(t, err)
}

func Test_Graph_AddRelationship_NoTo(t *testing.T) {
	grf :=assets.NewGraph()
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	node := assets.Node{}
	_, err := grf.AddRelationship(aNode, node, "friends")
	assert.Error(t, err)
}

func Test_Graph_GetRelationship(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	initialRel, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	foundRel, err := grf.GetRelationshipByID(initialRel.ID)
	assert.NoError(t, err)
	assert.Equal(t, initialRel, foundRel)
}

func Test_Graph_GetRelationship_NotFound(t *testing.T) {
	grf :=assets.NewGraph()
	_, err := grf.GetRelationshipByID("fake")
	assert.Error(t, err)
	assert.ErrorIs(t, err, assets.ErrNotFound)
}

func Test_Graph_ListRelationships(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	dNode := grf.InsertNode(smaug, dragonType, smaugBody)
	rel1, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	rel2, err := grf.AddRelationship(bNode, aNode, "competitors")
	assert.NoError(t, err)
	rel3, err := grf.AddRelationship(bNode, dNode, "enemies")
	assert.NoError(t, err)
	rels := grf.ListRelationships()
	assert.Equal(t, 3, len(rels))
	assert.Contains(t, rels, rel1)
	assert.Contains(t, rels, rel2)
	assert.Contains(t, rels, rel3)
}

func Test_Graph_ListRelationships_Filter(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	dNode := grf.InsertNode(smaug, dragonType, smaugBody)
	rel1, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(bNode, aNode, "competitors")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(bNode, dNode, "enemies")
	assert.NoError(t, err)
	rels := grf.ListRelationships(assets.FilterRelByLabel("friends"))
	assert.Equal(t, 1, len(rels))
	assert.Contains(t, rels, rel1)
}

func Test_Graph_ListRelationships_FilterByFrom(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	dNode := grf.InsertNode(smaug, dragonType, smaugBody)
	rel1, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(dNode, bNode, "enemies")
	assert.NoError(t, err)
	rels := grf.ListRelationships(assets.FilterRelByFrom(bNode.GetID()))
	assert.Equal(t, 1, len(rels))
	assert.Contains(t, rels, rel1)
}

func Test_Graph_ListRelationships_FilterByTo(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	dNode := grf.InsertNode(smaug, dragonType, smaugBody)
	rel1, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(dNode, bNode, "enemies")
	assert.NoError(t, err)
	rels := grf.ListRelationships(assets.FilterRelByTo(aNode.GetID()))
	assert.Equal(t, 1, len(rels))
	assert.Contains(t, rels, rel1)
}

func Test_Graph_ListConnections(t *testing.T) {
	grf :=assets.NewGraph()
	bNode := grf.InsertNode(bobita, puppyType, bobitaBody)
	aNode := grf.InsertNode(azor, puppyType, azorBody)
	dNode := grf.InsertNode(smaug, dragonType, smaugBody)
	_, err := grf.AddRelationship(bNode, aNode, "friends")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(bNode, dNode, "enemies")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(aNode, dNode, "enemies")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(aNode, bNode, "friends")
	assert.NoError(t, err)
	_, err = grf.AddRelationship(aNode, bNode, "friends")
	assert.NoError(t, err)
	bNode, err = grf.GetNodeByID(bNode.GetID())
	assert.NoError(t, err)
	dNode, err = grf.GetNodeByID(dNode.GetID())
	assert.NoError(t, err)
	cons := grf.ListConnections(bNode, dNode)
	assert.Len(t, cons, 2)
	expectedRelationships := []string{
		"{Asset:Bobita}->{rel:Bobita-friends-Azor}->{Asset:Azor}->{rel:Azor-enemies-Smaug}->{Asset:Smaug}",
		"{Asset:Bobita}->{rel:Bobita-enemies-Smaug}->{Asset:Smaug}",
	}
	for _, v := range cons {
		assert.Contains(t, expectedRelationships, v.String())
	}
}
