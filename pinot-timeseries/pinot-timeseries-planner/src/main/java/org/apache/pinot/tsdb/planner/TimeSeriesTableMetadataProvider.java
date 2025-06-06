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
package org.apache.pinot.tsdb.planner;

import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tsdb.spi.TimeSeriesMetadata;


/**
 * Implementation of TimeSeriesTableMetadata that uses TableCache to provide table metadata.
 */
public class TimeSeriesTableMetadataProvider implements TimeSeriesMetadata {
  private final TableCache _tableCache;

  public TimeSeriesTableMetadataProvider(TableCache tableCache) {
    _tableCache = tableCache;
  }

  @Override
  @Nullable
  public TableConfig getTableConfig(String tableName) {
    return _tableCache.getTableConfig(tableName);
  }

  @Override
  @Nullable
  public Schema getSchema(String rawTableName) {
    return _tableCache.getSchema(rawTableName);
  }

  @Override
  @Nullable
  public String getActualTableName(String tableName) {
    return _tableCache.getActualTableName(tableName);
  }
}
