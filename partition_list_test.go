package tstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_partitionList_Remove(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl[float64]
		target            partition[float64]
		wantErr           bool
		wantPartitionList partitionListImpl[float64]
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl[float64]{},
			wantErr:       true,
		},
		{
			name: "remove the head node",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition[float64]{
				minT: 1,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 1,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				},
			},
		},
		{
			name: "remove the tail node",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition[float64]{
				minT: 2,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 1,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
				},
			},
		},
		{
			name: "remove the middle node",
			partitionList: func() partitionListImpl[float64] {
				third := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 3,
					},
				}
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
					next: third,
				}
				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			target: &fakePartition[float64]{
				minT: 2,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 2,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: &partitionNode[float64]{
						val: &fakePartition[float64]{
							minT: 3,
						},
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			target: &fakePartition[float64]{
				minT: 3,
			},
			wantPartitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.remove(tt.target)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}

func Test_partitionList_Swap(t *testing.T) {
	tests := []struct {
		name              string
		partitionList     partitionListImpl[float64]
		old               partition[float64]
		new               partition[float64]
		wantErr           bool
		wantPartitionList partitionListImpl[float64]
	}{
		{
			name:          "empty partition",
			partitionList: partitionListImpl[float64]{},
			wantErr:       true,
		},
		{
			name: "swap the head node",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition[float64]{
				minT: 1,
			},
			new: &fakePartition[float64]{
				minT: 100,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 2,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 100,
					},
					next: &partitionNode[float64]{
						val: &fakePartition[float64]{
							minT: 2,
						},
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				},
			},
		},
		{
			name: "swap the tail node",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition[float64]{
				minT: 2,
			},
			new: &fakePartition[float64]{
				minT: 100,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 2,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: &partitionNode[float64]{
						val: &fakePartition[float64]{
							minT: 100,
						},
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 100,
					},
				},
			},
		},
		{
			name: "swap the middle node",
			partitionList: func() partitionListImpl[float64] {
				third := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 3,
					},
				}
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
					next: third,
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 3,
					head:          first,
					tail:          third,
				}
			}(),
			old: &fakePartition[float64]{
				minT: 2,
			},
			new: &fakePartition[float64]{
				minT: 100,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 3,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: &partitionNode[float64]{
						val: &fakePartition[float64]{
							minT: 100,
						},
						next: &partitionNode[float64]{
							val: &fakePartition[float64]{
								minT: 3,
							},
						},
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 3,
					},
				},
			},
		},
		{
			name: "given node not found",
			partitionList: func() partitionListImpl[float64] {
				second := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				}

				first := &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: second,
				}
				return partitionListImpl[float64]{
					numPartitions: 2,
					head:          first,
					tail:          second,
				}
			}(),
			old: &fakePartition[float64]{
				minT: 100,
			},
			wantPartitionList: partitionListImpl[float64]{
				numPartitions: 2,
				head: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 1,
					},
					next: &partitionNode[float64]{
						val: &fakePartition[float64]{
							minT: 2,
						},
					},
				},
				tail: &partitionNode[float64]{
					val: &fakePartition[float64]{
						minT: 2,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.partitionList.swap(tt.old, tt.new)
			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.wantPartitionList, tt.partitionList)
		})
	}
}
