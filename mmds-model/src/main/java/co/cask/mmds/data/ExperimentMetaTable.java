package co.cask.mmds.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A thin layer on top of the underlying Table that stores the experiment data. Handles scanning, deletion,
 * serialization, deserialization, etc. This is not a custom dataset because custom datasets cannot currently
 * be used in plugins.
 */
public class ExperimentMetaTable {
  private final static String NAME_COL = "name";
  private final static String DESC_COL = "description";
  private final static String SRCPATH_COL = "srcpath";
  private final static String OUTCOME_COL = "outcome";
  private final static String OUTCOME_TYPE_COL = "outcomeType";
  private final static String WORKSPACE_COL = "workspace";
  private static final Schema SCHEMA = Schema.recordOf(
    "experiments",
    Schema.Field.of(NAME_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(DESC_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SRCPATH_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(OUTCOME_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(OUTCOME_TYPE_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(WORKSPACE_COL, Schema.of(Schema.Type.STRING))
  );
  public static final DatasetProperties DATASET_PROPERTIES = DatasetProperties.builder()
    .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, SRCPATH_COL)
    .add(Table.PROPERTY_SCHEMA, SCHEMA.toString())
    .add(Table.PROPERTY_SCHEMA_ROW_FIELD, NAME_COL).build();

  private final Table table;

  public ExperimentMetaTable(Table table) {
    this.table = table;
  }

  /**
   * List all experiments. Never returns null. If there are no experiments, returns an empty list.
   *
   * @return all experiments
   */
  public List<Experiment> list() {
    Scan scan = new Scan(null, null);

    List<Experiment> experiments = new ArrayList<>();
    try (Scanner scanner = table.scan(scan)) {
      Row row;
      while ((row = scanner.next()) != null) {
        experiments.add(fromRow(row));
      }
    }
    return experiments;
  }

  /**
   * Get information about the specified experiment.
   *
   * @param name the experiment name
   * @return information about the specified experiment
   */
  @Nullable
  public Experiment get(String name) {
    Row row = table.get(Bytes.toBytes(name));
    return row.isEmpty() ? null : fromRow(row);
  }

  /**
   * Delete the specified experiment
   *
   * @param name the experiment name
   */
  public void delete(String name) {
    table.delete(Bytes.toBytes(name));
  }

  /**
   * Add or update the specified experiment.
   *
   * @param experiment the experiment to write
   */
  public void put(Experiment experiment) {
    Put put = new Put(experiment.getName())
      .add(NAME_COL, experiment.getName())
      .add(DESC_COL, experiment.getDescription())
      .add(SRCPATH_COL, experiment.getSrcpath())
      .add(OUTCOME_COL, experiment.getOutcome())
      .add(OUTCOME_TYPE_COL, experiment.getOutcomeType())
      .add(WORKSPACE_COL, experiment.getWorkspaceId());
    table.put(put);
  }

  private Experiment fromRow(Row row) {
    return new Experiment(row.getString(NAME_COL), row.getString(DESC_COL), row.getString(SRCPATH_COL),
                          row.getString(OUTCOME_COL), row.getString(OUTCOME_TYPE_COL), row.getString(WORKSPACE_COL));
  }
}