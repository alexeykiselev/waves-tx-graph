package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wavesplatform/gowaves/pkg/proto"
)

func tID(order byte) proto.AddressID {
	r := proto.AddressID{}
	r[0] = order
	return r
}

func fourIDs() (proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID) {
	return tID(1), tID(2), tID(3), tID(4)
}

func nineIDs() (proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID, proto.AddressID) {
	return tID(1), tID(2), tID(3), tID(4), tID(5), tID(6), tID(7), tID(8), tID(9)
}

func gr(e ...proto.AddressID) []proto.AddressID {
	return append([]proto.AddressID{}, e...)
}

func gg(g ...[]proto.AddressID) [][]proto.AddressID {
	return append([][]proto.AddressID{}, g...)
}

func TestContains(t *testing.T) {
	a, b, c, d := fourIDs()
	for _, test := range []struct {
		a []proto.AddressID
		e proto.AddressID
		r bool
	}{
		{gr(), d, false},
		{gr(a), d, false},
		{gr(a, b), d, false},
		{gr(a, b, c), d, false},
		{gr(a, b, c, d), d, true},
		{gr(), a, false},
		{gr(a), a, true},
		{gr(a, b), a, true},
		{gr(a, b, c), a, true},
		{gr(a, b, c, d), a, true},
	} {
		assert.Equal(t, test.r, contains(test.a, test.e))
	}
}

func TestIntersects(t *testing.T) {
	a, b, c, d := fourIDs()
	for _, test := range []struct {
		a, b []proto.AddressID
		r    bool
	}{
		{gr(), gr(c, d), false},
		{gr(a), gr(c, d), false},
		{gr(a, b), gr(c, d), false},
		{gr(a, b, c), gr(c, d), true},
		{gr(a, b, c, d), gr(c, d), true},
		{gr(), gr(a, c), false},
		{gr(a), gr(a, c), true},
		{gr(a, b), gr(a, c), true},
		{gr(a, b, c), gr(a, c), true},
		{gr(a, b, c, d), gr(a, c), true},
	} {
		assert.Equal(t, test.r, intersects(test.a, test.b))
	}
}

func TestGroupTransactions(t *testing.T) {
	a, b, c, d, e, f, g, h, i := nineIDs()
	for _, test := range []struct {
		txs    [][]proto.AddressID
		ac, gc int
		su     float32
	}{
		{gg(gr(a, b), gr(c, d), gr(e, f)), 6, 3, 3},
		{gg(gr(a, b, c), gr(d, e, f), gr(g, h, i)), 9, 3, 3},
		{gg(gr(a, b), gr(c, d), gr(a, d)), 4, 1, 1},
		{gg(gr(a, b), gr(c, d), gr(e, f), gr(g, h), gr(a, d), gr(b, f)), 8, 2, 1.2},
		{gg(gr(a, b), gr(c, d), gr(e, f), gr(g, h), gr(a, d), gr(b, f), gr(i)), 9, 3, 1.4000001},
		{gg(gr(a, b), gr(c, d), gr(e, f), gr(g, h), gr(a, d), gr(b, f), gr(i), gr(a, c, e, g, i)), 9, 1, 1},
	} {
		ac, tc, gc, su := groupTransactions(test.txs)
		assert.Equal(t, test.ac, int(ac))
		assert.Equal(t, len(test.txs), int(tc))
		assert.Equal(t, test.gc, int(gc))
		assert.Equal(t, test.su, su)
	}
}
