package co.cask.mmds.modeler.param;

import java.util.Map;

/**
 * Params for random forest regression.
 */
public class RandomForestRegressionParams extends RandomForestParams {

  public RandomForestRegressionParams(Map<String, String> modelParams) {
    super(modelParams, "onethird");
  }
}
