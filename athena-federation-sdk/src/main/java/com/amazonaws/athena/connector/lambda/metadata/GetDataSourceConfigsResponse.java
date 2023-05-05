/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connector.lambda.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class GetDataSourceConfigsResponse
        extends MetadataResponse
{
    private final Map<String, String> configs;
    /**
     * Constructs a new MetadataResponse object.
     *
     * @param catalogName The catalog name that the metadata is for.
     * @param configs The string key-value map of configs
     */
    public GetDataSourceConfigsResponse(@JsonProperty("catalogName") String catalogName,
                                        @JsonProperty("configs") Map<String, String> configs)
    {
        super(MetadataRequestType.GET_DATASOURCE_CONFIGS, catalogName);
        requireNonNull(configs, "configs are null");
        this.configs = Collections.unmodifiableMap(configs);
    }

    public Map<String, String> getConfigs()
    {
        return configs;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                // do not print configs because it might contain sensitive information like credentials.
                .add("configs", "--redacted--")
                .add("requestType", getRequestType())
                .add("catalogName", getCatalogName())
                .toString();
    }

    @Override
    public void close()
            throws Exception
    {
        //No Op
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GetDataSourceConfigsResponse that = (GetDataSourceConfigsResponse) o;

        return Objects.equal(this.configs, that.configs) &&
                Objects.equal(this.getRequestType(), that.getRequestType()) &&
                Objects.equal(this.getCatalogName(), that.getCatalogName());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(configs, getRequestType(), getCatalogName());
    }
}
