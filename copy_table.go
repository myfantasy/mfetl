package mfetl

import (
	"context"
	"database/sql"

	"github.com/myfantasy/mfdb"

	"github.com/myfantasy/mfe"
)

// CopyTable -  copy table from any db to any sql db
func CopyTable(conf mfe.Variant) (err error) {

	mfe.LogActionF("", "mfetl.CopyTable", "Conf Load")

	dbTypeFrom := conf.GE("db_type_from").Str()
	dbFromCS := conf.GE("db_from").Str()
	dbTypeTo := conf.GE("db_type_to").Str()
	dbToCS := conf.GE("db_to").Str()
	queryFrom := conf.GE("query_from").Str()

	sbf := mfdb.SBulkFieldCreate(conf.GE("fields"))

	tableTo := conf.GE("table_to").Str()
	schemaTo := conf.GE("schema_to").Str()

	batchSize := mfe.CoalesceI(int(conf.GE("batch_size").Dec().IntPart()), 10000)

	mfe.LogActionF("", "mfetl.CopyTable", "Open Destination")

	dbTo, err := sql.Open(dbTypeTo, dbToCS)
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.CopyTable", "Open Destination")
		return err
	}
	defer dbTo.Close()

	mfe.LogActionF("", "mfetl.CopyTable", "Open Source")

	dbFrom, err := sql.Open(dbTypeFrom, dbFromCS)
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.CopyTable", "Open Source")
		return err
	}
	defer dbFrom.Close()

	ctx := context.Background()

	cFrom, err := dbFrom.Conn(ctx)
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.CopyTable", "Open Source Connection")
		return err
	}
	defer cFrom.Close()

	err = mfdb.ExecuteBatchInConnection(ctx, cFrom, queryFrom, batchSize, func(v mfe.Variant) (err error) {
		mfe.LogActionF("", "mfetl.CopyTable", "Start Write Batch")
		cTo, e := dbTo.Conn(ctx)
		if e != nil {
			mfe.LogExtErrorF(e.Error(), "mfetl.CopyTable", "Open Source Connection")
			return e
		}
		defer cTo.Close()

		if dbTypeTo == "sqlserver" {
			e = MSCopy(ctx, cTo, tableTo, sbf, v)
		} else if dbTypeTo == "postgres" {
			e = PGCopy(ctx, cTo, schemaTo, tableTo, sbf, v)
		} else {
			query := mfdb.InsertQuery(&v, tableTo)
			_, e = mfdb.ExecuteInConnection(ctx, cTo, query)
		}

		return e
	})

	return err
}
