package mfetl

import (
	"context"
	"database/sql"
	"fmt"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/myfantasy/mfdb"
	"github.com/myfantasy/mfe"
)

// MSCopy copy to MS to table
func MSCopy(ctx context.Context, c *sql.Conn, tableName string, sbf mfdb.SBulkField, v mfe.Variant) (err error) {

	txn, err := c.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	fields := sbf.Columns()

	stmt, err := txn.Prepare(mssql.CopyIn(tableName, mssql.BulkOptions{}, fields...))
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.MSCopy", "Prepare")
		return err
	}

	for _, vi := range v.SV() {
		ins := sbf.Values(vi)
		_, err = stmt.Exec(ins...)
		if err != nil {
			mfe.LogExtErrorF(err.Error(), "mfetl.MSCopy", "item add")
			return err
		}
	}

	result, err := stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.MSCopy", "Prepare")
		return err
	}

	err = txn.Commit()
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.MSCopy", "Prepare")
		return err
	}

	mfe.LogActionF("Rows: "+fmt.Sprint(result), "mfetl.MSCopy", "Done")

	return nil
}
