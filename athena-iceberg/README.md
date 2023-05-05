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