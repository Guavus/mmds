package co.cask.mmds.stats;

import java.io.Serializable;

/**
 * Numeric stats that can be calculated in one pass.
 */
public class NumericStats implements Serializable {
  private static final long serialVersionUID = 6770304235190586763L;
  private Double min;
  private Double max;
  private long count;
  private long nullCount;

  public NumericStats(Double val) {
    this(val, val, 1, val == null ? 1L : 0L);
  }

  private NumericStats(Double min, Double max, long count, long nullCount) {
    this.min = min;
    this.max = max;
    this.count = count;
    this.nullCount = nullCount;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  public long getCount() {
    return count;
  }

  public long getNullCount() {
    return nullCount;
  }

  public void update(Double val) {
    count++;
    if (val == null) {
      nullCount++;
      return;
    }
    if (min == null) {
      min = val;
    } else {
      min = Math.min(min, val);
    }
    if (max == null) {
      max = val;
    } else {
      max = Math.max(max, val);
    }
  }

  public NumericStats merge(NumericStats other) {
    Double newMin;
    Double newMax;
    if (min != null && other.min != null) {
      newMin = Math.min(min, other.min);
    } else if (min == null) {
      newMin = other.min;
    } else {
      newMin = min;
    }
    if (max != null && other.max != null) {
      newMax = Math.max(max, other.max);
    } else if (max == null) {
      newMax = other.max;
    } else {
      newMax = max;
    }
    return new NumericStats(newMin, newMax, count + other.count, nullCount + other.nullCount);
  }
}
