/*-
 * #%L
 * Amazon Athena GCS Connector
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
package com.amazonaws.athena.connectors.gcs.filter;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.gcs.GcsTestUtils;
import com.amazonaws.services.glue.model.Column;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*",
        "javax.management.*", "org.w3c.*", "javax.net.ssl.*", "sun.security.*", "jdk.internal.reflect.*", "javax.crypto.*"
})
@PrepareForTest({GcsTestUtils.class})
public class FilterExpressionBuilderTest
{
    @Test
    public void testGetExpressions()
    {
        Map<String, java.util.Optional<java.util.Set<String>>> result = FilterExpressionBuilder.getConstraintsForPartitionedColumns(
            com.google.common.collect.ImmutableList.of(new Column().withName("year")),
            new Constraints(createSummaryWithLValueRangeEqual("year", new ArrowType.Utf8(), "1")));
        assertEquals(result.size(), 1);
        assertEquals(result.get("year").get(), com.google.common.collect.ImmutableSet.of("1"));
        assertEquals(result.get("yeAr").get(), com.google.common.collect.ImmutableSet.of("1"));
    }

    public static Map<String, ValueSet> createSummaryWithLValueRangeEqual(String fieldName, ArrowType fieldType, Object fieldValue)
    {
        Block block = Mockito.mock(Block.class);
        FieldReader fieldReader = Mockito.mock(FieldReader.class);
        Mockito.when(fieldReader.getField()).thenReturn(Field.nullable(fieldName, fieldType));

        Mockito.when(block.getFieldReader(anyString())).thenReturn(fieldReader);
        Marker low = Marker.exactly(new BlockAllocatorImpl(), new ArrowType.Utf8(), fieldValue);
        return com.google.common.collect.ImmutableMap.of(
                fieldName, SortedRangeSet.of(false, new Range(low, low))
        );
    }
}
