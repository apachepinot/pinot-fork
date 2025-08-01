/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils.config;

import java.io.File;
import java.net.URL;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;


public class SchemaSerDeUtilsTest {

  @Test
  public void testSerDe()
      throws Exception {
    URL resourceUrl = getClass().getClassLoader().getResource("schemaTest.schema");
    assertNotNull(resourceUrl);
    Schema schema = Schema.fromFile(new File(resourceUrl.getFile()));

    Schema schemaToCompare = Schema.fromString(schema.toPrettyJsonString());
    assertEquals(schemaToCompare, schema);
    assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    schemaToCompare = Schema.fromString(schema.toSingleLineJsonString());
    assertEquals(schemaToCompare, schema);
    assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    schemaToCompare = SchemaSerDeUtils.fromZNRecord(SchemaSerDeUtils.toZNRecord(schema));
    assertEquals(schemaToCompare, schema);
    assertEquals(schemaToCompare.hashCode(), schema.hashCode());

    // When setting new fields, schema string should be updated
    String jsonSchema = schemaToCompare.toSingleLineJsonString();
    schemaToCompare.setSchemaName("newSchema");
    String jsonSchemaToCompare = schemaToCompare.toSingleLineJsonString();
    assertNotEquals(jsonSchemaToCompare, jsonSchema);
  }
}
