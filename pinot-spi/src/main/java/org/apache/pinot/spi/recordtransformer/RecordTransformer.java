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
package org.apache.pinot.spi.recordtransformer;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The record transformer which takes a {@link GenericRow} and transform it based on some custom rules.
 */
public interface RecordTransformer extends Serializable {

  /**
   * Returns {@code true} if the transformer is no-op (can be skipped), {@code false} otherwise.
   */
  default boolean isNoOp() {
    return false;
  }

  /**
   * Returns the input columns required for the transformer. This is used to make sure the required input fields are
   * extracted.
   */
  default Collection<String> getInputColumns() {
    return List.of();
  }

  /**
   * Transforms a record based on some custom rules.
   *
   * @param record Record to transform
   * @return Transformed record, or {@code null} if the record does not follow certain rules.
   */
  @Nullable
  GenericRow transform(GenericRow record);
}
