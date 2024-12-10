package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/0xPolygon/cdk-data-availability/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
)

const (
	// storeLastProcessedBlockSQL is a query that stores the last processed block for a given task
	storeLastProcessedBlockSQL = `
		UPDATE data_node.sync_tasks
    	SET block = $2, processed = NOW()
    	WHERE task = $1;`

	// getLastProcessedBlockSQL is a query that returns the last processed block for a given task
	getLastProcessedBlockSQL = `SELECT block FROM data_node.sync_tasks WHERE task = $1;`

	// getMissingBatchKeysSQL is a query that returns the missing batch keys from the database
	getMissingBatchKeysSQL = `SELECT num, hash FROM data_node.missing_batches LIMIT $1;`

	// getOffchainDataSQL is a query that returns the offchain data for a given key
	getOffchainDataSQL = `
		SELECT key, value
		FROM data_node.offchain_data 
		WHERE key = $1 LIMIT 1;
	`

	// listOffchainDataSQL is a query that returns the offchain data for a given list of keys
	listOffchainDataSQL = `
		SELECT key, value
		FROM data_node.offchain_data 
		WHERE key IN (?);
	`

	// countOffchainDataSQL is a query that returns the count of rows in the offchain_data table
	countOffchainDataSQL = "SELECT COUNT(*) FROM data_node.offchain_data;"
)

var (
	// ErrStateNotSynchronized indicates the state database may be empty
	ErrStateNotSynchronized = errors.New("state not synchronized")
)

// DB defines functions that a DB instance should implement
type DB interface {
	StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error
	GetLastProcessedBlock(ctx context.Context, task string) (uint64, error)

	StoreMissingBatchKeys(ctx context.Context, bks []types.BatchKey) error
	GetMissingBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error)
	DeleteMissingBatchKeys(ctx context.Context, bks []types.BatchKey) error

	GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error)
	ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error)
	StoreOffChainData(ctx context.Context, od []types.OffChainData) error
	CountOffchainData(ctx context.Context) (uint64, error)
}

// DB is the database layer of the data node
type pgDB struct {
	pg *sqlx.DB

	storeLastProcessedBlockStmt *sqlx.Stmt
	getLastProcessedBlockStmt   *sqlx.Stmt
	getMissingBatchKeysStmt     *sqlx.Stmt
	getOffChainDataStmt         *sqlx.Stmt
	countOffChainDataStmt       *sqlx.Stmt
}

// New instantiates a DB
func New(ctx context.Context, pg *sqlx.DB) (DB, error) {
	storeLastProcessedBlockStmt, err := pg.PreparexContext(ctx, storeLastProcessedBlockSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the store last processed block statement: %w", err)
	}

	getLastProcessedBlockStmt, err := pg.PreparexContext(ctx, getLastProcessedBlockSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get last processed block statement: %w", err)
	}

	getMissingBatchKeysStmt, err := pg.PreparexContext(ctx, getMissingBatchKeysSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get missing batch keys statement: %w", err)
	}

	getOffChainDataStmt, err := pg.PreparexContext(ctx, getOffchainDataSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the get offchain data statement: %w", err)
	}

	countOffChainDataStmt, err := pg.PreparexContext(ctx, countOffchainDataSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare the count offchain data statement: %w", err)
	}

	return &pgDB{
		pg:                          pg,
		storeLastProcessedBlockStmt: storeLastProcessedBlockStmt,
		getLastProcessedBlockStmt:   getLastProcessedBlockStmt,
		getMissingBatchKeysStmt:     getMissingBatchKeysStmt,
		getOffChainDataStmt:         getOffChainDataStmt,
		countOffChainDataStmt:       countOffChainDataStmt,
	}, nil
}

// StoreLastProcessedBlock stores a record of a block processed by the synchronizer for named task
func (db *pgDB) StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error {
	_, err := db.storeLastProcessedBlockStmt.ExecContext(ctx, task, block)
	return err
}

// GetLastProcessedBlock returns the latest block successfully processed by the synchronizer for named task
func (db *pgDB) GetLastProcessedBlock(ctx context.Context, task string) (uint64, error) {
	var lastBlock uint64

	if err := db.getLastProcessedBlockStmt.QueryRowContext(ctx, task).Scan(&lastBlock); err != nil {
		return 0, err
	}

	return lastBlock, nil
}

// StoreMissingBatchKeys stores missing batch keys in the database
func (db *pgDB) StoreMissingBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	if len(bks) == 0 {
		return nil
	}

	query, args := buildBatchKeysInsertQuery(bks)

	if _, err := db.pg.ExecContext(ctx, query, args...); err != nil {
		batchNumbers := make([]string, len(bks))
		for i, bk := range bks {
			batchNumbers[i] = fmt.Sprintf("%d", bk.Number)
		}
		return fmt.Errorf("failed to store missing batches (batch numbers: %s): %w", strings.Join(batchNumbers, ", "), err)
	}

	return nil
}

// GetMissingBatchKeys returns the missing batch keys that is not yet present in offchain table
func (db *pgDB) GetMissingBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error) {
	rows, err := db.getMissingBatchKeysStmt.QueryxContext(ctx, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	type row struct {
		Number uint64 `db:"num"`
		Hash   string `db:"hash"`
	}

	var bks []types.BatchKey
	for rows.Next() {
		bk := row{}
		if err = rows.StructScan(&bk); err != nil {
			return nil, err
		}

		bks = append(bks, types.BatchKey{
			Number: bk.Number,
			Hash:   common.HexToHash(bk.Hash),
		})
	}

	return bks, nil
}

// DeleteMissingBatchKeys deletes the missing batch keys from the missing_batch table in the db
func (db *pgDB) DeleteMissingBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	if len(bks) == 0 {
		return nil
	}

	const columnsAffected = 2

	args := make([]interface{}, len(bks)*columnsAffected)
	values := make([]string, len(bks))
	for i, bk := range bks {
		values[i] = fmt.Sprintf("($%d, $%d)", i*columnsAffected+1, i*columnsAffected+2) //nolint:mnd
		args[i*columnsAffected] = bk.Number
		args[i*columnsAffected+1] = bk.Hash.Hex()
	}

	query := fmt.Sprintf(`
		DELETE FROM data_node.missing_batches WHERE (num, hash) IN (%s);
	`, strings.Join(values, ","))

	if _, err := db.pg.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to delete missing batches: %w", err)
	}

	return nil
}

// StoreOffChainData stores and array of key values in the Db
func (db *pgDB) StoreOffChainData(ctx context.Context, ods []types.OffChainData) error {
	if len(ods) == 0 {
		return nil
	}

	query, args := buildOffchainDataInsertQuery(ods)
	if _, err := db.pg.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to store offchain data: %w", err)
	}

	return nil
}

// GetOffChainData returns the value identified by the key
func (db *pgDB) GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error) {
	data := struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}{}

	if err := db.getOffChainDataStmt.QueryRowxContext(ctx, key.Hex()).StructScan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrStateNotSynchronized
		}

		return nil, err
	}

	return &types.OffChainData{
		Key:   common.HexToHash(data.Key),
		Value: common.FromHex(data.Value),
	}, nil
}

// ListOffChainData returns values identified by the given keys
func (db *pgDB) ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	preparedKeys := make([]string, len(keys))
	for i, key := range keys {
		preparedKeys[i] = key.Hex()
	}

	query, args, err := sqlx.In(listOffchainDataSQL, preparedKeys)
	if err != nil {
		return nil, err
	}

	// sqlx.In returns queries with the `?` bindvar, we can rebind it for our backend
	query = db.pg.Rebind(query)

	rows, err := db.pg.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	if rows != nil {
		defer rows.Close()
	}

	type row struct {
		Key   string `db:"key"`
		Value string `db:"value"`
	}

	list := make([]types.OffChainData, 0, len(keys))
	for rows.Next() {
		data := row{}
		if err = rows.StructScan(&data); err != nil {
			return nil, err
		}

		list = append(list, types.OffChainData{
			Key:   common.HexToHash(data.Key),
			Value: common.FromHex(data.Value),
		})
	}

	return list, nil
}

// CountOffchainData returns the count of rows in the offchain_data table
func (db *pgDB) CountOffchainData(ctx context.Context) (uint64, error) {
	var count uint64
	if err := db.countOffChainDataStmt.QueryRowContext(ctx).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

// buildBatchKeysInsertQuery builds the query to insert missing batch keys
func buildBatchKeysInsertQuery(bks []types.BatchKey) (string, []interface{}) {
	const columnsAffected = 2

	args := make([]interface{}, len(bks)*columnsAffected)
	values := make([]string, len(bks))
	for i, bk := range bks {
		values[i] = fmt.Sprintf("($%d, $%d)", i*columnsAffected+1, i*columnsAffected+2) //nolint:mnd
		args[i*columnsAffected] = bk.Number
		args[i*columnsAffected+1] = bk.Hash.Hex()
	}

	return fmt.Sprintf(`
		INSERT INTO data_node.missing_batches (num, hash)
		VALUES %s
		ON CONFLICT (num, hash) DO NOTHING;
	`, strings.Join(values, ",")), args
}

// buildOffchainDataInsertQuery builds the query to insert offchain data
func buildOffchainDataInsertQuery(ods []types.OffChainData) (string, []interface{}) {
	const columnsAffected = 2

	// Remove duplicates from the given offchain data
	ods = types.RemoveDuplicateOffChainData(ods)

	args := make([]interface{}, len(ods)*columnsAffected)
	values := make([]string, len(ods))
	for i, od := range ods {
		values[i] = fmt.Sprintf("($%d, $%d)", i*columnsAffected+1, i*columnsAffected+2) //nolint:mnd
		args[i*columnsAffected] = od.Key.Hex()
		args[i*columnsAffected+1] = common.Bytes2Hex(od.Value)
	}

	return fmt.Sprintf(`
		INSERT INTO data_node.offchain_data (key, value)
		VALUES %s
		ON CONFLICT (key) DO UPDATE 
		SET value = EXCLUDED.value;
	`, strings.Join(values, ",")), args
}
