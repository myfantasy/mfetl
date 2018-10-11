#mfetl

tools for etl


## RunMethod
run methods by param "method"

## Examples
#### Copy Table
pg to pg
```json
{
    "method":"copy",
    "db_type_from":"postgres",
    "db_from":"host=host_from port=5432 dbname=db_name user=some_user password=super_secret_password sslmode=disable",
    "db_type_to":"postgres",
    "db_to":"host=host_to port=5432 dbname=db_name_to user=some_user_to password=super_secret_password_to sslmode=disable",
    "query_from":"select delivery_id, status_id, user_id from logistics.delivery limit 100000;",
    "table_to":"delivery_tst",
    "schema_to":"logistics",
    "fields":[
        {
            "name":"delivery_id",
            "type":"int64"
        },
        {
            "name":"status_id",
            "type":"int"
        }
    ]    
}
```
pg to sql
```json
{
    "method":"copy",
    "db_type_from":"postgres",
    "db_from":"host=host_from port=5432 dbname=db_name user=some_user password=super_secret_password sslmode=disable",
    "db_type_to":"sqlserver",
    "db_to":"server=host_to;user id=domain\\some_user_to;password=super_secret_password_to;port=1433;database=db_name_to",
    "query_from":"select delivery_id, status_id, user_id from logistics.delivery limit 100000;",
    "table_to":"logistics.delivery_tst",
    "schema_to":"",
    "fields":[
        {
            "name":"delivery_id",
            "type":"int64"
        },
        {
            "name":"status_id",
            "type":"int"
        }
    ]    
}
```


