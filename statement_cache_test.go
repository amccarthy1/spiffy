package spiffy

import (
	"testing"

	assert "github.com/blendlabs/go-assert"
)

func TestStatementCachePrepare(t *testing.T) {
	assert := assert.New(t)

	sc := NewStatementCache(DefaultDb().Connection)
	query := "select 'ok'"
	stmt, err := sc.Prepare(query)

	assert.Nil(err)
	assert.NotNil(stmt)
	assert.True(sc.HasStatement(query))

	// shoul result in cache hit
	stmt, err = sc.Prepare(query)
	assert.NotNil(stmt)
	assert.True(sc.HasStatement(query))
}
