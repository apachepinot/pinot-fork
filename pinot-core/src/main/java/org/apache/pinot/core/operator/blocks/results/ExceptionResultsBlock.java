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
package org.apache.pinot.core.operator.blocks.results;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.exception.QueryErrorMessage;


public class ExceptionResultsBlock extends BaseResultsBlock {
  public ExceptionResultsBlock(QueryErrorMessage queryErrorMessage) {
    addErrorMessage(queryErrorMessage);
  }

  @Override
  public int getNumRows() {
    return 0;
  }

  @Nullable
  @Override
  public QueryContext getQueryContext() {
    return null;
  }

  @Nullable
  @Override
  public DataSchema getDataSchema() {
    return null;
  }

  @Nullable
  @Override
  public List<Object[]> getRows() {
    return null;
  }

  @Override
  public DataTable getDataTable() {
    return DataTableBuilderFactory.getEmptyDataTable();
  }
}
