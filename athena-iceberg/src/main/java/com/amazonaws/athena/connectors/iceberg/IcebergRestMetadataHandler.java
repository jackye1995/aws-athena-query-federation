/*-
 * #%L
 * athena-iceberg
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.iceberg;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.MetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceConfigsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceConfigsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class IcebergRestMetadataHandler
        extends MetadataHandler
{
    private static final Logger logger = LoggerFactory.getLogger(IcebergRestMetadataHandler.class);

    private static final String CATALOG_NAME = "athena_iceberg_rest";
    private static final String ICEBERG_REST_URI_ENV = "iceberg_rest_uri";
    private static final String ICEBERG_REST_WAREHOUSE_ENV = "iceberg_rest_warehouse";
    private static final String ICEBERG_REST_CREDENTIAL_ENV = "iceberg_rest_credential";
    private static final String ICEBERG_REST_HEADERS_ENV = "iceberg_rest_headers";

    private static final Set<String> REDACTED_CONFIGS = Set.of(OAuth2Properties.CREDENTIAL, OAuth2Properties.TOKEN);

    private final Map<String, String> configs;
    private final RESTCatalog restCatalog;

    public IcebergRestMetadataHandler()
    {
        this(System.getenv());
    }

    public IcebergRestMetadataHandler(Map<String, String> configOptions)
    {
        super(CATALOG_NAME, configOptions);
        this.restCatalog = new RESTCatalog();
        this.configs = new HashMap<>();
        configs.put(CatalogProperties.URI, requireNonNull(
                configOptions.get(ICEBERG_REST_URI_ENV),
                ICEBERG_REST_URI_ENV + " must be set"));
        configs.put(CatalogProperties.WAREHOUSE_LOCATION, requireNonNull(
                configOptions.get(ICEBERG_REST_WAREHOUSE_ENV),
                ICEBERG_REST_WAREHOUSE_ENV + " must be set"));
        configs.put(OAuth2Properties.CREDENTIAL, requireNonNull(
                configOptions.get(ICEBERG_REST_CREDENTIAL_ENV),
                ICEBERG_REST_CREDENTIAL_ENV + " must be set"));

        if (configOptions.containsKey(ICEBERG_REST_HEADERS_ENV)) {
            String[] parts = configOptions.get(ICEBERG_REST_HEADERS_ENV).split(",");
            for (String part : parts) {
                String[] keyVal = part.split("=");
                if (keyVal.length != 2) {
                    throw new IllegalArgumentException("Bad header configuration: " + part);
                }
                configs.put(keyVal[0], keyVal[1]);
            }
        }

        logger.info("Initializing REST catalog with configs: {}",
                configs.entrySet().stream()
                        .map(e -> {
                            if (REDACTED_CONFIGS.contains(e.getKey())) {
                                return e.getKey() + "=REDACTED";
                            } else {
                                return e.getKey() + "=" + e.getValue();
                            }
                        })
                        .collect(Collectors.toList()));

        restCatalog.initialize(CATALOG_NAME, configs);

    }

    @Override
    public GetDataSourceConfigsResponse doGetDataSourceConfigs(
            BlockAllocator allocator,
            GetDataSourceConfigsRequest request)
    {
        logger.info("doGetDataSourceConfigs: enter - " + request);
        return new GetDataSourceConfigsResponse(request.getCatalogName(), configs);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator allocator, ListSchemasRequest request)
    {
        logger.info("doListSchemaNames: enter - " + request);
        List<String> schemas = restCatalog.listNamespaces().stream()
                .map(Namespace::toString)
                .collect(Collectors.toList());
        return new ListSchemasResponse(request.getCatalogName(), schemas);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator allocator, ListTablesRequest request)
    {
        logger.info("doListTables: enter - " + request);
        Namespace namespace = fromSchemaName(request.getSchemaName());
        List<TableName> tables = restCatalog.listTables(namespace).stream()
                .map(id -> new TableName(request.getSchemaName(), id.name()))
                .collect(Collectors.toList());
        return new ListTablesResponse(request.getCatalogName(), tables, null);
    }

    @Override
    public GetTableResponse doGetTable(BlockAllocator allocator, GetTableRequest request)
    {
        logger.info("doGetTable: enter - " + request);
        Namespace namespace = fromSchemaName(request.getTableName().getSchemaName());
        Table table = restCatalog.loadTable(TableIdentifier.of(namespace, request.getTableName().getTableName()));
        Schema schema = ArrowSchemaUtil.convert(table.schema());
        if (table.spec().isUnpartitioned()) {
            return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
        }
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema,
                table.spec().fields().stream()
                        .map(PartitionField::sourceId)
                        .map(id -> table.schema().findField(id).name())
                        .collect(Collectors.toSet()));
    }

    @Override
    public void getPartitions(
            BlockWriter blockWriter,
            GetTableLayoutRequest request,
            QueryStatusChecker queryStatusChecker)
    {
        throw new UnsupportedOperationException("Unsupported operation: GetPartitions");
    }

    @Override
    public GetSplitsResponse doGetSplits(
            BlockAllocator allocator,
            GetSplitsRequest request)
    {
        throw new UnsupportedOperationException("Unsupported operation: GetSplits");
    }

    private Namespace fromSchemaName(String schema) {
        return Namespace.of(schema.split("\\."));
    }
}
