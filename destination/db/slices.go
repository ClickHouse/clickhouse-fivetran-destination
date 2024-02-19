package db

import (
	"fmt"
)

// Slice
// Num of the slice in the group (required for ClickHouseConnection.SelectByPrimaryKeys)
// Start index in the file
// End index in the file
type Slice struct {
	Num   uint
	Start uint
	End   uint
}

// GroupSlices
// For example, if `fileLen` = 40, `batchSize` = 10, and `maxParallelOperations` = 2,
// the result will be:
//
//		{
//	        // Parallel group #1
//			{
//				{Num: 0, Start: 0, End: 9},   // Slice #1 of group #1
//				{Num: 1, Start: 10, End: 19}, // Slice #2 of group #1
//			},
//	        // Parallel group #2
//			{
//				{Num: 3, Start: 20, End: 29}, // Slice #1 of group #2
//				{Num: 4, Start: 30, End: 39}, // Slice #2 of group #2
//			},
//		}
//
// See the tests for more examples.
//
// See usage in:
// - ClickHouseConnection.SelectByPrimaryKeys (since SELECT is limited by the size of the query and amount of IN values)
// - ClickHouseConnection.UpdateBatch / ClickHouseConnection.SoftDeleteBatch (batching for sequential operations)
func GroupSlices(fileLen uint, batchSize uint, maxParallelOperations uint) ([][]Slice, error) {
	if maxParallelOperations == 0 {
		return nil, fmt.Errorf("maxParallelOperations can't be zero")
	}
	if batchSize == 0 {
		return nil, fmt.Errorf("batchSize can't be zero")
	}
	if fileLen == 0 {
		return nil, nil
	}
	groupsCount := uint(0)
	if fileLen%(batchSize*maxParallelOperations) > 0 {
		groupsCount = (fileLen / batchSize / maxParallelOperations) + 1
	} else {
		groupsCount = fileLen / batchSize / maxParallelOperations
	}
	groups := make([][]Slice, groupsCount)
	for i := uint(0); i < groupsCount; i++ {
		groups[i] = make([]Slice, 0, maxParallelOperations)
		for j := uint(0); j < maxParallelOperations; j++ {
			start := i*maxParallelOperations*batchSize + j*batchSize
			if start >= fileLen {
				break
			}
			end := start + batchSize
			if end > fileLen {
				end = fileLen
			}
			groups[i] = append(groups[i], Slice{Num: j + i*maxParallelOperations, Start: start, End: end})
		}
	}
	return groups, nil
}
