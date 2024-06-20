package synchronizer

import (
	"context"
	"errors"
	"testing"

	"github.com/0xPolygon/cdk-data-availability/db"
	"github.com/0xPolygon/cdk-data-availability/mocks"
	"github.com/0xPolygon/cdk-data-availability/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_getStartBlock(t *testing.T) {
	testError := errors.New("test error")

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		block   uint64
		wantErr bool
	}{
		{
			name: "GetLastProcessedBlock returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("GetLastProcessedBlock", mock.Anything, "L1").
					Return(uint64(0), testError)

				return mockDB
			},
			block:   0,
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("GetLastProcessedBlock", mock.Anything, "L1").Return(uint64(5), nil)

				return mockDB
			},
			block: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if block, err := getStartBlock(context.Background(), testDB); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.block, block)
			}
		})
	}
}

func Test_setStartBlock(t *testing.T) {
	testError := errors.New("test error")

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		block   uint64
		wantErr bool
	}{
		{
			name: "BeginStateTransaction returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("BeginStateTransaction", mock.Anything).
					Return(nil, testError)

				return mockDB
			},
			block:   1,
			wantErr: true,
		},
		{
			name: "StoreLastProcessedBlock returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreLastProcessedBlock", mock.Anything, "L1", uint64(2), mockTx).
					Return(testError)

				return mockDB
			},
			block:   2,
			wantErr: true,
		},
		{
			name: "Commit returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreLastProcessedBlock", mock.Anything, "L1", uint64(3), mockTx).
					Return(nil)

				mockTx.On("Commit").
					Return(testError)

				return mockDB
			},
			block:   3,
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreLastProcessedBlock", mock.Anything, "L1", uint64(4), mockTx).
					Return(nil)

				mockTx.On("Commit").
					Return(nil)

				return mockDB
			},
			block: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if err := setStartBlock(context.Background(), testDB, tt.block); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_exists(t *testing.T) {
	tests := []struct {
		name string
		db   func(t *testing.T) db.DB
		key  common.Hash
		want bool
	}{
		{
			name: "Exists returns true",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("Exists", mock.Anything, common.HexToHash("0x01")).
					Return(true)

				return mockDB
			},
			key:  common.HexToHash("0x01"),
			want: true,
		},
		{
			name: "Exists returns false",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("Exists", mock.Anything, common.HexToHash("0x02")).
					Return(false)

				return mockDB
			},
			key:  common.HexToHash("0x02"),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			got := exists(context.Background(), testDB, tt.key)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_storeUnresolvedBatchKeys(t *testing.T) {
	testError := errors.New("test error")
	testData := []types.BatchKey{
		{
			Number: 1,
			Hash:   common.HexToHash("0x01"),
		},
	}

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		keys    []types.BatchKey
		wantErr bool
	}{
		{
			name: "BeginStateTransaction returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(nil, testError)

				return mockDB
			},
			keys:    testData,
			wantErr: true,
		},
		{
			name: "StoreUnresolvedBatchKeys returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreUnresolvedBatchKeys", mock.Anything, testData, mockTx).Return(testError)

				mockTx.On("Rollback").Return(nil)

				return mockDB
			},
			keys:    testData,
			wantErr: true,
		},
		{
			name: "Commit returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreUnresolvedBatchKeys", mock.Anything, testData, mockTx).Return(nil)

				mockTx.On("Commit").Return(testError)

				return mockDB
			},
			keys:    testData,
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreUnresolvedBatchKeys", mock.Anything, testData, mockTx).Return(nil)

				mockTx.On("Commit").Return(nil)

				return mockDB
			},
			keys: testData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if err := storeUnresolvedBatchKeys(context.Background(), testDB, tt.keys); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_getUnresolvedBatchKeys(t *testing.T) {
	testError := errors.New("test error")
	testData := []types.BatchKey{
		{
			Number: 1,
			Hash:   common.HexToHash("0x01"),
		},
	}

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		keys    []types.BatchKey
		wantErr bool
	}{
		{
			name: "GetUnresolvedBatchKeys returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("GetUnresolvedBatchKeys", mock.Anything).
					Return(nil, testError)

				return mockDB
			},
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("GetUnresolvedBatchKeys", mock.Anything).Return(testData, nil)

				return mockDB
			},
			keys: testData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if keys, err := getUnresolvedBatchKeys(context.Background(), testDB); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.keys, keys)
			}
		})
	}
}

func Test_deleteUnresolvedBatchKeys(t *testing.T) {
	testError := errors.New("test error")
	testData := []types.BatchKey{
		{
			Number: 1,
			Hash:   common.HexToHash("0x01"),
		},
	}

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		wantErr bool
	}{
		{
			name: "BeginStateTransaction returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("BeginStateTransaction", mock.Anything).
					Return(nil, testError)

				return mockDB
			},
			wantErr: true,
		},
		{
			name: "DeleteUnresolvedBatchKeys returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("DeleteUnresolvedBatchKeys", mock.Anything, testData, mockTx).
					Return(testError)

				mockTx.On("Rollback").Return(nil)

				return mockDB
			},
			wantErr: true,
		},
		{
			name: "Commit returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("DeleteUnresolvedBatchKeys", mock.Anything, testData, mockTx).
					Return(nil)

				mockTx.On("Commit").
					Return(testError)

				return mockDB
			},
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("DeleteUnresolvedBatchKeys", mock.Anything, testData, mockTx).
					Return(nil)

				mockTx.On("Commit").
					Return(nil)

				return mockDB
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if err := deleteUnresolvedBatchKeys(context.Background(), testDB, testData); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_storeOffchainData(t *testing.T) {
	testError := errors.New("test error")
	testData := []types.OffChainData{
		{
			Key:   common.HexToHash("0x01"),
			Value: []byte("test data 1"),
		},
	}

	tests := []struct {
		name    string
		db      func(t *testing.T) db.DB
		data    []types.OffChainData
		wantErr bool
	}{
		{
			name: "BeginStateTransaction returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(nil, testError)

				return mockDB
			},
			data:    testData,
			wantErr: true,
		},
		{
			name: "StoreOffChainData returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreOffChainData", mock.Anything, testData, mockTx).Return(testError)

				mockTx.On("Rollback").Return(nil)

				return mockDB
			},
			data:    testData,
			wantErr: true,
		},
		{
			name: "Commit returns error",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreOffChainData", mock.Anything, testData, mockTx).Return(nil)

				mockTx.On("Commit").Return(testError)

				return mockDB
			},
			data:    testData,
			wantErr: true,
		},
		{
			name: "all good",
			db: func(t *testing.T) db.DB {
				mockDB := mocks.NewDB(t)

				mockTx := mocks.NewTx(t)

				mockDB.On("BeginStateTransaction", mock.Anything).Return(mockTx, nil)

				mockDB.On("StoreOffChainData", mock.Anything, testData, mockTx).Return(nil)

				mockTx.On("Commit").Return(nil)

				return mockDB
			},
			data: testData,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testDB := tt.db(t)

			if err := storeOffchainData(context.Background(), testDB, tt.data); tt.wantErr {
				require.ErrorIs(t, err, testError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
