/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.options;

import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.DoubleValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;

import java.util.Iterator;

public abstract class BaseOptionManager implements OptionManager {
  /**
   * Gets the current option value given a validator.
   *
   * @param validator the validator
   * @return option value
   * @throws IllegalArgumentException - if the validator is not found
   */
  private OptionValue getOptionSafe(OptionValidator validator)  {
    OptionValue value = getOption(validator.getOptionName());
    return value == null ? validator.getDefault() : value;
  }

  @Override
  public boolean getOption(BooleanValidator validator) {
    return getOptionSafe(validator).bool_val;
  }

  @Override
  public double getOption(DoubleValidator validator) {
    return getOptionSafe(validator).float_val;
  }

  @Override
  public long getOption(LongValidator validator) {
    return getOptionSafe(validator).num_val;
  }

  @Override
  public String getOption(StringValidator validator) {
    return getOptionSafe(validator).string_val;
  }

  @Override
  public OptionList getInternalOptionList() {
    return getAllOptionList(true);
  }

  @Override
  public OptionList getExternalOptionList() {
    return getAllOptionList(false);
  }

  private OptionList getAllOptionList(boolean internal)
  {
    Iterator<OptionValue> values = this.iterator();
    OptionList optionList = new OptionList();

    while (values.hasNext()) {
      OptionValue value = values.next();

      if (getOptionValidator(value.getName()).getMetaData().isInternal() == internal) {
        optionList.add(value);
      }
    }

    return optionList;
  }
}
