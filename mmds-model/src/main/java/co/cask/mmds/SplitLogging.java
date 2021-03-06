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

package co.cask.mmds;

import org.slf4j.MDC;

/**
 * Sets MDC keys to tag log statements for a data split.
 */
public class SplitLogging {
  private static final String SPLIT_KEY = "MDC:split";
  private static final String EXPERIMENT_KEY = "MDC:experiment";

  public static void start(String experiment, String split) {
    MDC.put(SPLIT_KEY, split);
    MDC.put(EXPERIMENT_KEY, experiment);
  }

  public static void finish() {
    MDC.remove(SPLIT_KEY);
    MDC.remove(EXPERIMENT_KEY);
  }
}
