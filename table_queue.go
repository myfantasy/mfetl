package mfetl

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/myfantasy/mfdb"
	"github.com/myfantasy/mfe"
)

// TableQueue - copy data from source table or query to destincation table or query and remove info from source table and mark as ready
func TableQueue(conf mfe.Variant) (err error) {
	mfe.LogActionF("", "mfetl.TableQueue", "Conf Load")

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

	return TableQueueConnections(ctx, cFrom, cTo, dbTypeFrom, dbTypeTo, conf)
}

// TableQueueConnections - copy data from source table or query to destincation table or query and remove info from source table and mark as ready
func TableQueueConnections(ctx context.Context, cFrom *sql.Conn, cTo *sql.Conn, dbTypeFrom string, dbTypeTo string, conf mfe.Variant) (err error) {

	mfe.LogActionF("", "mfetl.TableQueueConnections", "Conf Load")

	a := true
	var e error

	for a && e == nil {
		mfe.LogActionF("", "mfetl.TableQueueConnections", "Clear Old")
		a, e = tableQueueGetCompletedRowsAndClear(ctx, cFrom, cTo, dbTypeFrom, dbTypeTo, conf)
		if e != nil {
			return e
		}
	}

	mfe.LogActionF("", "mfetl.TableQueueConnections", "Copy")
	e = CopyTableConnections(ctx, cFrom, cTo, dbTypeFrom, dbTypeTo, conf)
	if e != nil {
		return e
	}

	for a && e == nil {
		mfe.LogActionF("", "mfetl.TableQueueConnections", "Clear New")
		a, e = tableQueueGetCompletedRowsAndClear(ctx, cFrom, cTo, dbTypeFrom, dbTypeTo, conf)
		if e != nil {
			return e
		}
	}

	return err
}

func tableQueueGetCompletedRowsAndClear(ctx context.Context, cFrom *sql.Conn, cTo *sql.Conn,
	dbTypeFrom string, dbTypeTo string, conf mfe.Variant) (any bool, err error) {

	idName := mfe.CoalesceS(conf.GE("id_name").Str(), "_id")
	isReady := mfe.CoalesceS(conf.GE("is_ready_name").Str(), "_is_ready")

	limit := mfe.CoalesceI(conf.GE("limit").Int(), 10000)

	tableFrom := conf.GE("table_from").Str()
	tableTo := conf.GE("table_to").Str()
	schemaTo := conf.GE("schema_to").Str()

	srcIDTempPreDoFrom := conf.GE("ids_temp_pre_do_from").Str()
	srcIDTempPreDoTo := conf.GE("ids_temp_pre_do_to").Str()
	srcIDTempTableFrom := conf.GE("ids_temp_table_from").Str()
	srcIDTempTableTo := conf.GE("ids_temp_table_to").Str()

	if dbTypeFrom == "postgres" {
		tableTo = schemaTo + "." + tableTo
	}

	queryGetDestIds := conf.GE("dest_query_get_ids").Str()

	if queryGetDestIds == "" {
		if dbTypeFrom == "sqlserver" {
			queryGetDestIds = "select top (" + fmt.Sprint(limit) + ") " + idName + " from " + tableTo + " where " + isReady + " = 0;"
		} else {
			queryGetDestIds = "select " + idName + " from " + tableTo + " order by " + idName + " limit " + fmt.Sprint(limit) + ";"
		}
	}

	queryMarkIdsInSource := conf.GE("src_query_mark_ids").Str()

	if queryMarkIdsInSource == "" {
		if dbTypeFrom == "sqlserver" {
			queryMarkIdsInSource =
				"delete t from " + tableFrom + " t inner join #ids s on t." + idName + " = s." + idName + ";"
		} else if dbTypeFrom == "postgres" {
			queryMarkIdsInSource =
				"delete from " + tableFrom + " where " + idName + " = any({arr_ids});"
		}
	}

	queryMarkIdsInDestination := conf.GE("dst_query_mark_ids").Str()

	if queryMarkIdsInDestination == "" {
		if dbTypeFrom == "sqlserver" {
			queryMarkIdsInDestination =
				"delete t from " + tableTo + " t inner join #ids s on t." + idName + " = s." + idName + ";"
		} else if dbTypeFrom == "postgres" {
			queryMarkIdsInDestination =
				"delete from " + tableTo + " where " + idName + " = any({arr_ids});"
		}
	}

	batchSize := mfe.CoalesceI(int(conf.GE("batch_size").Int()), 10000)

	mfe.LogActionF("", "mfetl.tableQueueGetCompletedRowsAndClear", "Open Destination")

	any = false

	vIDs := mfe.VariantNewSV()

	err = mfdb.ExecuteBatchInConnection(ctx, cTo, queryGetDestIds, batchSize, func(v mfe.Variant) (err error) {
		mfe.LogActionF("", "mfetl.tableQueueGetCompletedRowsAndClear", "Start Write Batch")

		any = any || v.Count() > 0

		vIDs.AddRange(&v)

		e := queueIdsClear(ctx, cFrom, v, dbTypeFrom,
			idName, queryMarkIdsInSource, srcIDTempPreDoFrom, srcIDTempTableFrom)

		return e
	})

	if err != nil {
		return any, err
	}
	err = queueIdsClear(ctx, cTo, vIDs, dbTypeTo,
		idName, queryMarkIdsInDestination, srcIDTempPreDoTo, srcIDTempTableTo)

	return any, err

}

func queueIdsClear(ctx context.Context, c *sql.Conn, v mfe.Variant, dbType string,
	idName string, queryMarkIds string, idTempPreDo string, idTempTable string) (e error) {

	if dbType == "sqlserver" {
		_, e = mfdb.ExecuteInConnection(ctx, c, "create table #ids("+idName+" bigint);")

		if e != nil {
			return e
		}

		e = MSCopy(ctx, c, "#ids", mfdb.SBulkFieldCreateString(idName, "int64"), v)

		if e != nil {
			return e
		}

		_, e = mfdb.ExecuteInConnection(ctx, c, queryMarkIds)

		if e != nil {
			return e
		}

		_, e = mfdb.ExecuteInConnection(ctx, c, "drop table #ids;")
	} else if dbType == "postgres" {
		q := strings.Replace(queryMarkIds, "{arr_ids}", mfdb.Array(&v, idName), -1)
		_, e = mfdb.ExecuteInConnection(ctx, c, q)

		if e != nil {
			return e
		}
	} else {
		_, e = mfdb.ExecuteInConnection(ctx, c, idTempPreDo)

		if e != nil {
			return e
		}

		query := mfdb.InsertQuery(&v, idTempTable)
		_, e = mfdb.ExecuteInConnection(ctx, c, query)

		if e != nil {
			return e
		}

		_, e = mfdb.ExecuteInConnection(ctx, c, queryMarkIds)
	}

	return e
}
