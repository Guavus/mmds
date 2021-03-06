/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.mmds.spec;

import java.util.Map;

/**
 * An integer Modeler parameter.
 */
public class LongParam extends Param<Long> {
  private final ParamSpec spec;

  public LongParam(String name, String label, String description, Long defaultVal,
                   Range range, Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("long", name, label, description, String.valueOf(defaultVal), null, range);
  }

  @Override
  protected Long parseVal(String strVal) {
    try {
      return Long.parseLong(strVal);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(
        String.format("Invalid modeler parameter %s=%s. Must be a valid long.", name, strVal));
    }
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
