package mfetl

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/myfantasy/mfdb"
	"github.com/myfantasy/mfe"
)

// PGCopy copy to PG to table
func PGCopy(ctx context.Context, c *sql.Conn, schemaName string, tableName string, sbf mfdb.SBulkField, v mfe.Variant) (err error) {

	txn, err := c.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	fields := sbf.Columns()

	stmt, err := txn.Prepare(pq.CopyInSchema(schemaName, tableName, fields...))
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.PGCopy", "Prepare")
		return err
	}

	for _, vi := range v.SV() {
		ins := sbf.Values(vi)
		_, err = stmt.Exec(ins...)
		if err != nil {
			mfe.LogExtErrorF(err.Error(), "mfetl.PGCopy", "item add")
			return err
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.PGCopy", "Prepare")
		return err
	}

	err = txn.Commit()
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.PGCopy", "Prepare")
		return err
	}

	mfe.LogActionF("Rows: "+fmt.Sprint(result), "mfetl.PGCopy", "Done")

	return nil
}
