# Athena Iceberg REST Connector

This connector is used to read and write against Iceberg REST catalog.

To build, first run at the parent directory:

```shell
./mvnw install -DskipTests
```

Or if you have built the whole project before, just run inside this directory:

```shell
../mvnw install -DskipTests -pl :athena-iceberg-rest
```

To deploy, you need SAM. If it is not available install it [here](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).
Then run:

```shell
sam package --s3-bucket yzhaoqin-iceberg-eu-west-1 --region eu-west-1
sam deploy --guided
```

## Testing

You can add the following testing events in Lambda:

ListSchemas:

```json
{
  "@type": "ListSchemasRequest",
  "identity": {
    "id": "xxx",
    "principal": "xxx",
    "account": "12345",
    "arn": "xxx",
    "tags": {},
    "groups": []
  },
  "queryId": "xxx",
  "catalogName": "athena_iceberg_rest"
}
```

ListTables:

```json
{
  "@type": "ListTablesRequest",
  "identity": {
    "id": "xxx",
    "principal": "xxx",
    "account": "12345",
    "arn": "xxx",
    "tags": {},
    "groups": []
  },
  "queryId": "xxx",
  "catalogName": "athena_iceberg_rest",
  "schemaName": "examples"
}
```

GetTable:

```json
{
  "@type": "GetTableRequest",
  "identity": {
    "id": "xxx",
    "principal": "xxx",
    "account": "12345",
    "arn": "xxx",
    "tags": {},
    "groups": []
  },
  "queryId": "xxx",
  "catalogName": "athena_iceberg_rest",
  "tableName": {
      "schemaName": "examples",
      "tableName": "nyc_taxi_yellow"
  }
}
```

GetDataSourceConfigs:

```json
{
  "@type": "GetDataSourceConfigsRequest",
  "identity": {
    "id": "xxx",
    "principal": "xxx",
    "account": "12345",
    "arn": "xxx",
    "tags": {},
    "groups": []
  },
  "queryId": "xxx",
  "catalogName": "athena_iceberg_rest"
}
```