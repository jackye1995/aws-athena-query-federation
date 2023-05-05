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
package com.amazonaws.athena.connector.lambda.serde.v4;

import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceConfigsResponse;
import com.amazonaws.athena.connector.lambda.request.FederationResponse;
import com.amazonaws.athena.connector.lambda.serde.TypedDeserializer;
import com.amazonaws.athena.connector.lambda.serde.TypedSerializer;
import com.amazonaws.athena.connector.lambda.serde.VersionedSerDe;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.util.Map;

public class GetDataSourceConfigsResponseSerDeV4
{
    private static final String CATALOG_NAME_FIELD = "catalogName";
    private static final String CONFIGS_FIELD = "configs";

    private GetDataSourceConfigsResponseSerDeV4() {}

    public static final class Serializer extends TypedSerializer<FederationResponse> implements VersionedSerDe.Serializer<FederationResponse>
    {
        public Serializer()
        {
            super(FederationResponse.class, GetDataSourceConfigsResponse.class);
        }

        @Override
        protected void doTypedSerialize(FederationResponse federationResponse, JsonGenerator jgen, SerializerProvider provider)
                throws IOException
        {
            GetDataSourceConfigsResponse response = (GetDataSourceConfigsResponse) federationResponse;
            jgen.writeStringField(CATALOG_NAME_FIELD, response.getCatalogName());
            writeStringMap(jgen, CONFIGS_FIELD, response.getConfigs());
        }
    }

    public static final class Deserializer extends TypedDeserializer<FederationResponse> implements VersionedSerDe.Deserializer<FederationResponse>
    {
        public Deserializer()
        {
            super(FederationResponse.class, GetDataSourceConfigsResponse.class);
        }

        @Override
        protected FederationResponse doTypedDeserialize(JsonParser jparser, DeserializationContext ctxt)
                throws IOException
        {
            String catalogName = getNextStringField(jparser, CATALOG_NAME_FIELD);
            Map<String, String> configs = getNextStringMap(jparser, CONFIGS_FIELD);
            return new GetDataSourceConfigsResponse(catalogName, configs);
        }
    }
}
