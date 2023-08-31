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
	id2 := f.Getid()
	id3 := f.Getid()
	id4 := f.Getid()
	id5 := f.Getid()
	id6 := f.Getid()
	id7 := f.Getid()
	id1b, _ := ParseBytes(id1.ToBytes())
	id2b, _ := ParseBytes(id2.ToBytes())
	id3b, _ := ParseBytes(id3.ToBytes())
	id4b, _ := ParseBytes(id4.ToBytes())
	id5b, _ := ParseBytes(id5.ToBytes())
	id6b, _ := ParseBytes(id6.ToBytes())
	id7b, _ := ParseBytes(id7.ToBytes())

	t.Log(id1)
	t.Log(id1b)
	assert.Equal(t, id1, id1b)
	assert.Equal(t, id2, id2b)
	assert.Equal(t, id3, id3b)
	assert.Equal(t, id4, id4b)
	assert.Equal(t, id5, id5b)
	assert.Equal(t, id6, id6b)
	assert.Equal(t, id7, id7b)
}
