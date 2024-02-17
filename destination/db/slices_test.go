package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupSlices(t *testing.T) {
	res, err := GroupSlices(0, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(res))

	res, err = GroupSlices(2, 1, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 1}, // slice 1 in group 1
		},
		{ // parallel group 2
			{Num: 1, Start: 1, End: 2}, // slice 1 in group 2
		},
	}, res)

	res, err = GroupSlices(1, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 1}, // slice 1 in group 1
		},
	}, res)

	res, err = GroupSlices(2, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 2}, // slice 1 in group 1
		},
	}, res)

	res, err = GroupSlices(3, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 2}, // slice 1 in group 1
			{Num: 1, Start: 2, End: 3}, // slice 2 in group 1
		},
	}, res)

	res, err = GroupSlices(4, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 2}, // slice 1 in group 1
			{Num: 1, Start: 2, End: 4}, // slice 2 in group 1
		},
	}, res)

	res, err = GroupSlices(5, 2, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 2}, // slice 1 in group 1
			{Num: 1, Start: 2, End: 4}, // slice 2 in group 1
		},
		{ // parallel group 2
			{Num: 2, Start: 4, End: 5}, // slice 1 in group 2
		},
	}, res)

	res, err = GroupSlices(500, 100, 2)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 100},   // slice 1 in group 1
			{Num: 1, Start: 100, End: 200}, // slice 2 in group 1
		},
		{ // parallel group 2
			{Num: 2, Start: 200, End: 300}, // slice 1 in group 2
			{Num: 3, Start: 300, End: 400}, // slice 2 in group 2
		},
		{ // parallel group 3
			{Num: 4, Start: 400, End: 500}, // slice 1 in group 3
		},
	}, res)

	res, err = GroupSlices(10000, 1000, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 1000},    // slice 1 in group 1
			{Num: 1, Start: 1000, End: 2000}, // slice 2 in group 1
			{Num: 2, Start: 2000, End: 3000}, // slice 3 in group 1
			{Num: 3, Start: 3000, End: 4000}, // slice 4 in group 1
			{Num: 4, Start: 4000, End: 5000}, // slice 5 in group 1
		},
		{ // parallel group 2
			{Num: 5, Start: 5000, End: 6000},  // slice 1 in group 2
			{Num: 6, Start: 6000, End: 7000},  // slice 2 in group 2
			{Num: 7, Start: 7000, End: 8000},  // slice 3 in group 2
			{Num: 8, Start: 8000, End: 9000},  // slice 4 in group 2
			{Num: 9, Start: 9000, End: 10000}, // slice 5 in group 2
		},
	}, res)

	res, err = GroupSlices(999, 100, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 100},   // slice 1 in group 1
			{Num: 1, Start: 100, End: 200}, // slice 2 in group 1
			{Num: 2, Start: 200, End: 300}, // slice 3 in group 1
			{Num: 3, Start: 300, End: 400}, // slice 4 in group 1
			{Num: 4, Start: 400, End: 500}, // slice 5 in group 1
		},
		{ // parallel group 2
			{Num: 5, Start: 500, End: 600}, // slice 1 in group 2
			{Num: 6, Start: 600, End: 700}, // slice 2 in group 2
			{Num: 7, Start: 700, End: 800}, // slice 3 in group 2
			{Num: 8, Start: 800, End: 900}, // slice 4 in group 2
			{Num: 9, Start: 900, End: 999}, // slice 5 in group 2
		},
	}, res)

	res, err = GroupSlices(1001, 100, 5)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // parallel group 1
			{Num: 0, Start: 0, End: 100},   // slice 1 in group 1
			{Num: 1, Start: 100, End: 200}, // slice 2 in group 1
			{Num: 2, Start: 200, End: 300}, // slice 3 in group 1
			{Num: 3, Start: 300, End: 400}, // slice 4 in group 1
			{Num: 4, Start: 400, End: 500}, // slice 5 in group 1
		},
		{ // parallel group 2
			{Num: 5, Start: 500, End: 600},  // slice 1 in group 2
			{Num: 6, Start: 600, End: 700},  // slice 2 in group 2
			{Num: 7, Start: 700, End: 800},  // slice 3 in group 2
			{Num: 8, Start: 800, End: 900},  // slice 4 in group 2
			{Num: 9, Start: 900, End: 1000}, // slice 5 in group 2
		},
		{ // parallel group 3
			{Num: 10, Start: 1000, End: 1001}, // slice 1 in group 3
		},
	}, res)

	res, err = GroupSlices(300_000, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // only one slice in each group
			{Num: 0, Start: 0, End: 100_000},
		},
		{
			{Num: 1, Start: 100_000, End: 200_000},
		},
		{
			{Num: 2, Start: 200_000, End: 300_000},
		},
	}, res)

	res, err = GroupSlices(299_999, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // only one slice in each group
			{Num: 0, Start: 0, End: 100_000},
		},
		{
			{Num: 1, Start: 100_000, End: 200_000},
		},
		{
			{Num: 2, Start: 200_000, End: 299_999},
		},
	}, res)

	res, err = GroupSlices(300_001, 100_000, 1)
	assert.NoError(t, err)
	assert.Equal(t, [][]Slice{
		{ // only one slice in each group
			{Num: 0, Start: 0, End: 100_000},
		},
		{
			{Num: 1, Start: 100_000, End: 200_000},
		},
		{
			{Num: 2, Start: 200_000, End: 300_000},
		},
		{
			{Num: 3, Start: 300_000, End: 300_001},
		},
	}, res)

	_, err = GroupSlices(0, 0, 1)
	assert.ErrorContains(t, err, "batchSize can't be zero")

	_, err = GroupSlices(0, 1, 0)
	assert.ErrorContains(t, err, "maxParallelOperations can't be zero")
}
