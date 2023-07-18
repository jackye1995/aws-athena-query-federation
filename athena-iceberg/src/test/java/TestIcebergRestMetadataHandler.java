/*-
 * #%L
 * athena-iceberg
 * %%
 * Copyright (C) 2019 - 2023 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connectors.iceberg.IcebergRestMetadataHandler;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestIcebergRestMetadataHandler {

    @Disabled("Need env variable setup")
    @Test
    public void test() {
        IcebergRestMetadataHandler handler = new IcebergRestMetadataHandler();
        ListTablesRequest request = new ListTablesRequest(null, "queryId", "tabular", "examples", null, 10);
        handler.doListTables(null, request);
    }
}
