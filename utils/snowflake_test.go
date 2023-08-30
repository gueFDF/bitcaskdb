package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToBytes(t *testing.T) {
	f, e := NewSonwflacke(1)
	assert.Nil(t, e)

	id1 := f.Getid()
	t.Log(id1)
	t.Log(id1.ToBytes())
}

func TestParBytes(t *testing.T) {
	f, e := NewSonwflacke(1)
	assert.Nil(t, e)

	id1 := f.Getid()
	id2, _ := ParseBytes(id1.ToBytes())
	t.Log(id1)
	t.Log(id2)
	assert.Equal(t, id1, id2)
}
