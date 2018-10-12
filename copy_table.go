package mfetl

import (
	"context"
	"database/sql"
	"fmt"

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

	ctx := context.Background()

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

	cFrom, err := dbFrom.Conn(ctx)
	if err != nil {
		mfe.LogExtErrorF(err.Error(), "mfetl.CopyTable", "Open Source Connection")
		return err
	}
	defer cFrom.Close()

	cTo, e := dbTo.Conn(ctx)
	if e != nil {
		mfe.LogExtErrorF(e.Error(), "mfetl.CopyTable", "Open Source Connection")
		return e
	}
	defer cTo.Close()

	return CopyTableConnections(ctx, cFrom, cTo, dbTypeFrom, dbTypeTo, conf)
}

// CopyTableConnections -  copy table from any db to any sql db
func CopyTableConnections(ctx context.Context, cFrom *sql.Conn, cTo *sql.Conn, dbTypeFrom string, dbTypeTo string, conf mfe.Variant) (err error) {

	mfe.LogActionF("", "mfetl.CopyTableConnections", "Conf Load")

	queryFrom := FromTableCopyQuery(conf, dbTypeFrom)

	sbf := mfdb.SBulkFieldCreate(conf.GE("fields"))

	tableTo := conf.GE("table_to").Str()
	schemaTo := conf.GE("schema_to").Str()

	batchSize := mfe.CoalesceI(int(conf.GE("batch_size").Int()), 10000)

	mfe.LogActionF("", "mfetl.CopyTable", "Open Destination")

	err = mfdb.ExecuteBatchInConnection(ctx, cFrom, queryFrom, batchSize, func(v mfe.Variant) (err error) {
		mfe.LogActionF("", "mfetl.CopyTable", "Start Write Batch")

		var e error

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

// FromTableCopyQuery - create query for get rows from source
func FromTableCopyQuery(conf mfe.Variant, dbTypeFrom string) (s string) {
	queryFrom := conf.GE("query_from").Str()

	if queryFrom == "" {

		sbf := mfdb.SBulkFieldCreate(conf.GE("fields"))

		tableFrom := conf.GE("table_from").Str()
		idName := mfe.CoalesceS(conf.GE("id_name").Str(), "_id")
		limit := mfe.CoalesceI(conf.GE("limit").Int(), 10000)

		var fieldsList string
		if sbf.Any() {
			fieldsList = mfe.JoinS(",", sbf.Columns()...)
		} else {
			fieldsList = "*"
		}

		if dbTypeFrom == "sqlserver" {
			queryFrom = "select top (" + fmt.Sprint(limit) + ") " + fieldsList + " from " + tableFrom + " order by " + idName + ";"
		} else {
			queryFrom = "select " + fieldsList + " from " + tableFrom + " order by " + idName + " limit " + fmt.Sprint(limit) + ";"
		}

	}

	return queryFrom
}
