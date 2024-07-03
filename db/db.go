package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/0xPolygon/cdk-data-availability/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jmoiron/sqlx"
)

var (
	// ErrStateNotSynchronized indicates the state database may be empty
	ErrStateNotSynchronized = errors.New("state not synchronized")
)

// DB defines functions that a DB instance should implement
type DB interface {
	StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error
	GetLastProcessedBlock(ctx context.Context, task string) (uint64, error)

	StoreUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error
	GetUnresolvedBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error)
	DeleteUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error

	GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error)
	ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error)
	StoreOffChainData(ctx context.Context, od []types.OffChainData) error

	CountOffchainData(ctx context.Context) (uint64, error)
}

// DB is the database layer of the data node
type pgDB struct {
	pg *sqlx.DB
}

// New instantiates a DB
func New(pg *sqlx.DB) DB {
	return &pgDB{
		pg: pg,
	}
}

// StoreLastProcessedBlock stores a record of a block processed by the synchronizer for named task
func (db *pgDB) StoreLastProcessedBlock(ctx context.Context, block uint64, task string) error {
	const storeLastProcessedBlockSQL = `
		INSERT INTO data_node.sync_tasks (task, block) 
		VALUES ($1, $2)
		ON CONFLICT (task) DO UPDATE 
		SET block = EXCLUDED.block, processed = NOW();
	`

	if _, err := db.pg.ExecContext(ctx, storeLastProcessedBlockSQL, task, block); err != nil {
		return err
	}

	return nil
}

// GetLastProcessedBlock returns the latest block successfully processed by the synchronizer for named task
func (db *pgDB) GetLastProcessedBlock(ctx context.Context, task string) (uint64, error) {
	const getLastProcessedBlockSQL = "SELECT block FROM data_node.sync_tasks WHERE task = $1;"

	var (
		lastBlock uint64
	)

	if err := db.pg.QueryRowContext(ctx, getLastProcessedBlockSQL, task).Scan(&lastBlock); err != nil {
		return 0, err
	}

	return lastBlock, nil
}

// StoreUnresolvedBatchKeys stores unresolved batch keys in the database
func (db *pgDB) StoreUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	const storeUnresolvedBatchesSQL = `
		INSERT INTO data_node.unresolved_batches (num, hash)
		VALUES ($1, $2)
		ON CONFLICT (num, hash) DO NOTHING;
	`

	tx, err := db.pg.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	for _, bk := range bks {
		if _, err = tx.ExecContext(
			ctx, storeUnresolvedBatchesSQL,
			bk.Number,
			bk.Hash.Hex(),
		); err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				return fmt.Errorf("%v: rollback caused by %v", txErr, err)
			}

			return err
		}
	}

	return tx.Commit()
}

// GetUnresolvedBatchKeys returns the unresolved batch keys from the database
func (db *pgDB) GetUnresolvedBatchKeys(ctx context.Context, limit uint) ([]types.BatchKey, error) {
	const getUnresolvedBatchKeysSQL = "SELECT num, hash FROM data_node.unresolved_batches LIMIT $1;"

	rows, err := db.pg.QueryxContext(ctx, getUnresolvedBatchKeysSQL, limit)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var bks []types.BatchKey
	for rows.Next() {
		bk := struct {
			Number uint64 `db:"num"`
			Hash   string `db:"hash"`
		}{}
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

// DeleteUnresolvedBatchKeys deletes the unresolved batch keys from the database
func (db *pgDB) DeleteUnresolvedBatchKeys(ctx context.Context, bks []types.BatchKey) error {
	const deleteUnresolvedBatchKeysSQL = `
		DELETE FROM data_node.unresolved_batches
		WHERE num = $1 AND hash = $2;
	`

	tx, err := db.pg.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	for _, bk := range bks {
		if _, err = tx.ExecContext(
			ctx, deleteUnresolvedBatchKeysSQL,
			bk.Number,
			bk.Hash.Hex(),
		); err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				return fmt.Errorf("%v: rollback caused by %v", txErr, err)
			}

			return err
		}
	}

	return tx.Commit()
}

// StoreOffChainData stores and array of key values in the Db
func (db *pgDB) StoreOffChainData(ctx context.Context, od []types.OffChainData) error {
	const storeOffChainDataSQL = `
		INSERT INTO data_node.offchain_data (key, value, batch_num)
		VALUES ($1, $2, $3)
		ON CONFLICT (key) DO UPDATE 
		SET value = EXCLUDED.value, batch_num = EXCLUDED.batch_num;
	`

	tx, err := db.pg.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	for _, d := range od {
		if _, err = tx.ExecContext(
			ctx, storeOffChainDataSQL,
			d.Key.Hex(),
			common.Bytes2Hex(d.Value),
			d.BatchNum,
		); err != nil {
			if txErr := tx.Rollback(); txErr != nil {
				return fmt.Errorf("%v: rollback caused by %v", txErr, err)
			}

			return err
		}
	}

	return tx.Commit()
}

// GetOffChainData returns the value identified by the key
func (db *pgDB) GetOffChainData(ctx context.Context, key common.Hash) (*types.OffChainData, error) {
	const getOffchainDataSQL = `
		SELECT key, value, batch_num
		FROM data_node.offchain_data 
		WHERE key = $1 LIMIT 1;
	`

	data := struct {
		Key      string `db:"key"`
		Value    string `db:"value"`
		BatchNum uint64 `db:"batch_num"`
	}{}

	if err := db.pg.QueryRowxContext(ctx, getOffchainDataSQL, key.Hex()).StructScan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrStateNotSynchronized
		}

		return nil, err
	}

	return &types.OffChainData{
		Key:      common.HexToHash(data.Key),
		Value:    common.FromHex(data.Value),
		BatchNum: data.BatchNum,
	}, nil
}

// ListOffChainData returns values identified by the given keys
func (db *pgDB) ListOffChainData(ctx context.Context, keys []common.Hash) ([]types.OffChainData, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	const listOffchainDataSQL = `
		SELECT key, value, batch_num
		FROM data_node.offchain_data 
		WHERE key IN (?);
	`

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

	defer rows.Close()

	list := make([]types.OffChainData, 0, len(keys))
	for rows.Next() {
		data := struct {
			Key      string `db:"key"`
			Value    string `db:"value"`
			BatchNum uint64 `db:"batch_num"`
		}{}
		if err = rows.StructScan(&data); err != nil {
			return nil, err
		}

		list = append(list, types.OffChainData{
			Key:      common.HexToHash(data.Key),
			Value:    common.FromHex(data.Value),
			BatchNum: data.BatchNum,
		})
	}

	return list, nil
}

// CountOffchainData returns the count of rows in the offchain_data table
func (db *pgDB) CountOffchainData(ctx context.Context) (uint64, error) {
	const countQuery = "SELECT COUNT(*) FROM data_node.offchain_data;"

	var count uint64
	if err := db.pg.QueryRowContext(ctx, countQuery).Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}
