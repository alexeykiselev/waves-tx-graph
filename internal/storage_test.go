package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestName(t *testing.T) {
	bi := blockInfo{
		Accounts:     12345678,
		Transactions: 78901234,
		Groups:       34567890,
		Parallelism:  4.56,
	}
	b, err := bi.marshal()
	require.NoError(t, err)
	bi2 := &blockInfo{}
	err = bi2.unmarshal(b)
	require.NoError(t, err)
	assert.Equal(t, bi, *bi2)
}
