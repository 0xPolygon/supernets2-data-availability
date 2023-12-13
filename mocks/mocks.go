package mocks

import (
	"context"
	"math/big"

	"github.com/0xPolygon/cdk-data-availability/db"
	"github.com/0xPolygon/cdk-data-availability/etherman"
	"github.com/0xPolygon/cdk-data-availability/etherman/smartcontracts/cdkvalidium"
	"github.com/0xPolygon/cdk-data-availability/rpc"
	"github.com/0xPolygon/cdk-data-availability/types"
	"github.com/0xPolygon/cdk-data-availability/types/interfaces"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	ethCommon "github.com/ethereum/go-ethereum/common"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/mock"
)

var _ db.DBInterface = (*DBMock)(nil)

// DBMock is a mock of DBInterface implementation
type DBMock struct {
	mock.Mock
}

// BeginStateTransaction is a mock function of the DBInterface
func (d *DBMock) BeginStateTransaction(ctx context.Context) (pgx.Tx, error) {
	args := d.Called(ctx)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(pgx.Tx), args.Error(1) //nolint:forcetypeassertion
}

// Exists is a mock function of the DBInterface
func (d *DBMock) Exists(ctx context.Context, key common.Hash) bool {
	args := d.Called(ctx, key)

	return args.Bool(0)
}

// GetLastProcessedBlock is a mock function of the DBInterface
func (d *DBMock) GetLastProcessedBlock(ctx context.Context, task string) (uint64, error) {
	args := d.Called(ctx, task)

	return args.Get(0).(uint64), args.Error(1) //nolint:forcetypeassertion
}

// GetOffChainData is a mock function of the DBInterface
func (d *DBMock) GetOffChainData(ctx context.Context, key common.Hash, dbTx pgx.Tx) (rpc.ArgBytes, error) {
	args := d.Called(ctx, key, dbTx)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(rpc.ArgBytes), args.Error(1) //nolint:forcetypeassertion
}

// StoreLastProcessedBlock is a mock function of the DBInterface
func (d *DBMock) StoreLastProcessedBlock(ctx context.Context, task string, block uint64, dbTx pgx.Tx) error {
	args := d.Called(ctx, task, block, dbTx)

	return args.Error(0)
}

// StoreOffChainData is a mock function of the DBInterface
func (d *DBMock) StoreOffChainData(ctx context.Context, od []types.OffChainData, dbTx pgx.Tx) error {
	args := d.Called(ctx, od, dbTx)

	return args.Error(0)
}

var _ interfaces.EthClient = (*EthClientMock)(nil)

// EthClientMock is a mock implementation of EthClient interface
type EthClientMock struct {
	mock.Mock
}

// BlockByNumber is a mock function of the EthClient
func (e *EthClientMock) BlockByNumber(ctx context.Context, number *big.Int) (*ethTypes.Block, error) {
	args := e.Called(ctx, number)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*ethTypes.Block), args.Error(1) //nolint:forcetypeassertion
}

// CodeAt is a mock function of the EthClient
func (e *EthClientMock) CodeAt(ctx context.Context, account ethCommon.Address, blockNumber *big.Int) ([]byte, error) {
	args := e.Called(ctx, account, blockNumber)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).([]byte), args.Error(1) //nolint:forcetypeassertion
}

var _ interfaces.EthClientFactory = (*EthClientFactoryMock)(nil)

// EthClientFactoryMock is a mock implementation of EthClientFactory interface
type EthClientFactoryMock struct {
	mock.Mock
}

// CreateEthClient is a mock function of the EthClientFactory
func (e *EthClientFactoryMock) CreateEthClient(ctx context.Context, url string) (interfaces.EthClient, error) {
	args := e.Called(ctx, url)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(interfaces.EthClient), args.Error(1) //nolint:forcetypeassertion
}

var _ pgx.Tx = (*TxMock)(nil)

// TxMock is a mock implementation of pgx.Tx interface
type TxMock struct {
	mock.Mock
}

// Begin is a mock function of the EthClientFactory
func (tx *TxMock) Begin(ctx context.Context) (pgx.Tx, error) {
	panic("not implemented")
}

// BeginFunc is a mock function of the EthClientFactory
func (tx *TxMock) BeginFunc(ctx context.Context, f func(pgx.Tx) error) (err error) {
	panic("not implemented")
}

// Commit is a mock function of the EthClientFactory
func (tx *TxMock) Commit(ctx context.Context) error {
	args := tx.Called(ctx)

	return args.Error(0)
}

// Rollback is a mock function of the EthClientFactory
func (tx *TxMock) Rollback(ctx context.Context) error {
	args := tx.Called(ctx)

	return args.Error(0)
}

// CopyFrom is a mock function of the EthClientFactory
func (tx *TxMock) CopyFrom(ctx context.Context, tableName pgx.Identifier,
	columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	panic("not implemented")
}

// SendBatch is a mock function of the EthClientFactory
func (tx *TxMock) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	panic("not implemented")
}

// LargeObjects is a mock function of the EthClientFactory
func (tx *TxMock) LargeObjects() pgx.LargeObjects {
	panic("not implemented")
}

// Prepare is a mock function of the EthClientFactory
func (tx *TxMock) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	panic("not implemented")
}

// Exec is a mock function of the EthClientFactory
func (tx *TxMock) Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	panic("not implemented")
}

// Query is a mock function of the EthClientFactory
func (tx *TxMock) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}

// QueryRow is a mock function of the EthClientFactory
func (tx *TxMock) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	panic("not implemented")
}

// QueryFunc is a mock function of the EthClientFactory
func (tx *TxMock) QueryFunc(ctx context.Context, sql string,
	args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	panic("not implemented")
}

// Conn is a mock function of the EthClientFactory
func (tx *TxMock) Conn() *pgx.Conn {
	panic("not implemented")
}

var _ etherman.IEtherman = (*EthermanMock)(nil)

// EthermanMock is a mock implementation of EthermanInterface
type EthermanMock struct {
	mock.Mock
}

// GetCurrentDataCommittee is a mock function of the EthermanInterface
func (e *EthermanMock) GetCurrentDataCommittee() (*etherman.DataCommittee, error) {
	args := e.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(*etherman.DataCommittee), args.Error(1) //nolint:forcetypeassert
}

// GetCurrentDataCommitteeMembers is a mock function of the EthermanInterface
func (e *EthermanMock) GetCurrentDataCommitteeMembers() ([]etherman.DataCommitteeMember, error) {
	panic("not implemented")
}

// GetTx is a mock function of the EthermanInterface
func (e *EthermanMock) GetTx(ctx context.Context, txHash common.Hash) (*ethTypes.Transaction, bool, error) {
	panic("not implemented")
}

// TrustedSequencer is a mock function of the EthermanInterface
func (e *EthermanMock) TrustedSequencer() (common.Address, error) {
	panic("not implemented")
}

// TrustedSequencerURL is a mock function of the EthermanInterface
func (e *EthermanMock) TrustedSequencerURL() (string, error) {
	panic("not implemented")
}

// HeaderByNumber is a mock function of the EthermanInterface
func (e *EthermanMock) HeaderByNumber(ctx context.Context, number *big.Int) (*ethTypes.Header, error) {
	panic("not implemented")
}

// FilterSequenceBatches is a mock function of the EthermanInterface
func (e *EthermanMock) FilterSequenceBatches(opts *bind.FilterOpts,
	numBatch []uint64) (*cdkvalidium.CdkvalidiumSequenceBatchesIterator, error) {
	panic("not implemented")
}
