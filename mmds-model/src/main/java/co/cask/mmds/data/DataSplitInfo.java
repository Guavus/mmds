package co.cask.mmds.data;

import java.util.Objects;

/**
 * Information required to create a data split.
 */
public class DataSplitInfo {
  private final String splitId;
  private final Experiment experiment;
  private final DataSplit dataSplit;

  public DataSplitInfo(String splitId, Experiment experiment, DataSplit dataSplit) {
    this.splitId = splitId;
    this.experiment = experiment;
    this.dataSplit = dataSplit;
  }

  public String getSplitId() {
    return splitId;
  }

  public Experiment getExperiment() {
    return experiment;
  }

  public DataSplit getDataSplit() {
    return dataSplit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataSplitInfo that = (DataSplitInfo) o;

    return Objects.equals(splitId, that.splitId) &&
      Objects.equals(experiment, that.experiment) &&
      Objects.equals(dataSplit, that.dataSplit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitId, experiment, dataSplit);
  }

  @Override
  public String toString() {
    return "DataSplitInfo{" +
      "splitId='" + splitId + '\'' +
      ", experiment=" + experiment +
      ", dataSplit=" + dataSplit +
      '}';
  }
}