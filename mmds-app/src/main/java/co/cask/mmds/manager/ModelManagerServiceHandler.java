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

package co.cask.mmds.manager;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.api.spark.service.SparkHttpServicePluginContext;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.AlertPublisher;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchAggregator;
import co.cask.cdap.etl.api.batch.BatchConfigurable;
import co.cask.cdap.etl.api.batch.BatchJoiner;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.api.lineage.field.FieldOperation;
import co.cask.cdap.etl.batch.BatchPhaseSpec;
import co.cask.cdap.etl.batch.BatchPipelineSpec;
import co.cask.cdap.etl.batch.BatchPipelineSpecGenerator;
import co.cask.cdap.etl.batch.DefaultAggregatorContext;
import co.cask.cdap.etl.batch.DefaultJoinerContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.Constants;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.cdap.etl.common.PipelinePhase;
import co.cask.cdap.etl.common.PipelineRuntime;
import co.cask.cdap.etl.common.SetMultimapCodec;
import co.cask.cdap.etl.common.submit.AggregatorContextProvider;
import co.cask.cdap.etl.common.submit.ContextProvider;
import co.cask.cdap.etl.common.submit.Finisher;
import co.cask.cdap.etl.common.submit.JoinerContextProvider;
import co.cask.cdap.etl.common.submit.SubmitterPlugin;
import co.cask.cdap.etl.planner.PipelinePlan;
import co.cask.cdap.etl.planner.PipelinePlanner;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.spark.batch.BasicSparkPluginContext;
import co.cask.cdap.etl.spark.batch.BatchSparkPipelineDriver;
import co.cask.cdap.etl.spark.batch.ETLSpark;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSinkFactory;
import co.cask.cdap.etl.spark.batch.SparkBatchSourceContext;
import co.cask.cdap.etl.spark.batch.SparkBatchSourceFactory;
import co.cask.cdap.etl.spark.batch.SparkBatchSourceSinkFactoryInfo;
import co.cask.cdap.etl.batch.InteractivePipelineSpecGenerator;
import co.cask.cdap.etl.spec.StageSpec;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.mmds.ModelLogging;
import co.cask.mmds.SplitLogging;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.data.DataSplit;
import co.cask.mmds.data.DataSplitInfo;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.Experiment;
import co.cask.mmds.data.ExperimentMetaTable;
import co.cask.mmds.data.ExperimentStore;
import co.cask.mmds.data.ModelKey;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.data.ModelTrainerInfo;
import co.cask.mmds.data.SortInfo;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.modeler.Modelers;
import co.cask.mmds.modeler.train.ModelOutput;
import co.cask.mmds.modeler.train.ModelOutputWriter;
import co.cask.mmds.modeler.train.ModelTrainer;
import co.cask.mmds.proto.BadRequestException;
import co.cask.mmds.proto.CreateModelRequest;
import co.cask.mmds.proto.DirectivesRequest;
import co.cask.mmds.proto.EndpointException;
import co.cask.mmds.proto.TrainModelRequest;
import co.cask.mmds.spec.ParamSpec;
import co.cask.mmds.splitter.DataSplitResult;
import co.cask.mmds.splitter.DatasetSplitter;
import co.cask.mmds.splitter.SplitterSpec;
import co.cask.mmds.splitter.Splitters;

/**
 * Model Service handler
 */
public class ModelManagerServiceHandler implements SparkHttpServiceHandler {
	private static final Logger LOG = LoggerFactory.getLogger(ModelManagerServiceHandler.class);
	private static final Gson GSON = new GsonBuilder()
			.registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
			.registerTypeAdapter(SetMultimap.class, new SetMultimapCodec<>())
		    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
//		    .registerTypeAdapter(DatasetInfo.class, new DatasetInfoTypeAdapter())
//		    .registerTypeAdapter(OutputFormatProvider.class, new OutputFormatProviderTypeAdapter())
//		    .registerTypeAdapter(InputFormatProvider.class, new InputFormatProviderTypeAdapter())
			.registerTypeAdapterFactory(new EnumStringTypeAdapterFactory()).serializeSpecialFloatingPointValues()
			.create();
//	private static final Set<String> VALID_ERROR_INPUTS = ImmutableSet.of(
//		    BatchSource.PLUGIN_TYPE, Transform.PLUGIN_TYPE, BatchAggregator.PLUGIN_TYPE, ErrorTransform.PLUGIN_TYPE);
	private static final Set<String> SUPPORTED_PLUGIN_TYPES = ImmutableSet.of(
		    BatchSource.PLUGIN_TYPE, BatchSink.PLUGIN_TYPE, Transform.PLUGIN_TYPE);
	
	private String modelMetaDataset;
	private String modelComponentsDataset;
	private String experimentMetaDataset;
	private String splitsDataset;
	private ModelOutputWriter modelOutputWriter;
	private SparkSession sparkSession;
	private SparkHttpServiceContext context;

	@Override
	public void initialize(SparkHttpServiceContext context) throws Exception {
		this.context = context;
		sparkSession = context.getSparkSession();
		Map<String, String> properties = context.getSpecification().getProperties();
		modelMetaDataset = properties.get("modelMetaDataset");
		modelComponentsDataset = properties.get("modelComponentsDataset");
		experimentMetaDataset = properties.get("experimentMetaDataset");
		splitsDataset = properties.get("splitsDataset");
		context.execute(datasetContext -> {
			FileSet modelComponents = datasetContext.getDataset(modelComponentsDataset);
			modelOutputWriter = new ModelOutputWriter(context.getAdmin(), context, modelComponents.getBaseLocation(),
					true);
		});
	}

	@Override
	public void destroy() {
		// no-op
	}

	@GET
	@Path("/splitters")
	public void listSplitters(HttpServiceRequest request, HttpServiceResponder responder) {
		List<SplitterSpec> splitters = new ArrayList<>();
		for (DatasetSplitter splitter : Splitters.getSplitters()) {
			splitters.add(splitter.getSpec());
		}
		responder.sendString(GSON.toJson(splitters));
	}

	@GET
	@Path("/splitters/{splitter}")
	public void getSplitter(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("splitter") String splitterType) {
		DatasetSplitter splitter = Splitters.getSplitter(splitterType);
		if (splitter == null) {
			responder.sendError(404, "Splitter " + splitterType + " not found.");
			return;
		}
		responder.sendString(GSON.toJson(splitter.getSpec()));
	}

	@GET
	@Path("/algorithms")
	public void listAlgorithms(HttpServiceRequest request, HttpServiceResponder responder) {
		List<AlgorithmSpec> algorithms = new ArrayList<>();
		for (Modeler modeler : Modelers.getModelers()) {
			List<ParamSpec> paramSpecs = modeler.getParams(new HashMap<>()).getSpec();
			algorithms.add(new AlgorithmSpec(modeler.getAlgorithm(), paramSpecs));
		}
		responder.sendString(GSON.toJson(algorithms));
	}

	@GET
	@Path("/algorithms/{algorithm}")
	public void getAlgorithm(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("algorithm") String algorithm) {
		Modeler modeler = Modelers.getModeler(algorithm);
		if (modeler == null) {
			responder.sendError(404, "Algorithm " + algorithm + " not found.");
			return;
		}
		List<ParamSpec> paramSpecs = modeler.getParams(new HashMap<>()).getSpec();
		responder.sendString(GSON.toJson(new AlgorithmSpec(modeler.getAlgorithm(), paramSpecs)));
	}

	/**
	 * Get List of {@link Experiment}s.
	 *
	 * @param request
	 *            http request
	 * @param responder
	 *            http response containing list of experiments as json string
	 */
	@GET
	@Path("/experiments")
	public void listExperiments(HttpServiceRequest request, HttpServiceResponder responder,
			final @QueryParam("offset") @DefaultValue("0") int offset,
			final @QueryParam("limit") @DefaultValue("20") int limit,
			final @QueryParam("srcPath") @DefaultValue("") String srcPath,
			final @QueryParam("sort") @DefaultValue("name asc") String sort) {


		runInTx(responder, store -> {
			validate(offset, limit);
			Predicate<Experiment> predicate = srcPath.isEmpty() ? null : e -> e.getSrcpath().equals(srcPath);
			SortInfo sortInfo = SortInfo.parse(sort);
			responder.sendString(GSON.toJson(store.listExperiments(offset, limit, predicate, sortInfo)));
		});
	}

	
	private void validate(int offset, int limit) {
		if (offset < 0) {
			throw new BadRequestException("Offset must be zero or a positive number");
		}

		if (limit <= 0) {
			throw new BadRequestException("Limit must be a positive number");
		}
	}

	@GET
	@Path("/experiments/{experiment-name}")
	public void getExperiment(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName) {
		runInTx(responder, store -> responder.sendString(GSON.toJson(store.getExperimentStats(experimentName))));
	}

	@PUT
	@Path("/experiments/{experiment-name}")
	public void putExperiment(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName) {
		runInTx(responder, store -> {
			try {
				Experiment experiment = GSON.fromJson(Bytes.toString(request.getContent()), Experiment.class);
				experiment.validate();
				Experiment experimentInfo = new Experiment(experimentName, experiment);
				store.putExperiment(experimentInfo);
				responder.sendStatus(200);
			} catch (IllegalArgumentException e) {
				throw new BadRequestException(e.getMessage());
			} catch (JsonSyntaxException e) {
				throw new BadRequestException(
						String.format("Problem occurred while parsing request body for Experiment: %s. "
								+ "Please provide valid json. Error: %s", experimentName, e.getMessage()));
			}
		});
	}

	@DELETE
	@Path("/experiments/{experiment-name}")
	public void deleteExperiment(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName) {
		runInTx(responder, store -> {
			store.deleteExperiment(experimentName);
			responder.sendStatus(200);
		});
	}

	@GET
	@Path("/experiments/{experiment-name}/models")
	public void listModels(HttpServiceRequest request, HttpServiceResponder responder,
			final @PathParam("experiment-name") String experimentName,
			final @QueryParam("offset") @DefaultValue("0") int offset,
			final @QueryParam("limit") @DefaultValue("20") int limit,
			final @QueryParam("sort") @DefaultValue("name asc") String sort) {
		runInTx(responder, store -> {
			validate(offset, limit);
			SortInfo sortInfo = SortInfo.parse(sort);
			responder.sendString(GSON.toJson(store.listModels(experimentName, offset, limit, sortInfo)));
		});
	}

	@GET
	@Path("/experiments/{experiment-name}/models/{model-id}")
	public void getModel(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("model-id") String modelId) {
		final ModelKey modelKey = new ModelKey(experimentName, modelId);
		runInTx(responder, store -> responder.sendString(GSON.toJson(store.getModel(modelKey))));
	}

	@GET
	@Path("/experiments/{experiment-name}/models/{model-id}/status")
	public void getModelStatus(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("model-id") String modelId) {
		final ModelKey modelKey = new ModelKey(experimentName, modelId);
		runInTx(responder, store -> {
			ModelMeta meta = store.getModel(modelKey);
			responder.sendString(GSON.toJson(meta.getStatus()));
		});
	}

	@POST
	@Path("/experiments/{experiment-name}/models")
	public void addModel(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName) {
		runInTx(responder, store -> {
			try {
				CreateModelRequest createRequest = GSON.fromJson(Bytes.toString(request.getContent()),
						CreateModelRequest.class);
				if (createRequest == null) {
					throw new BadRequestException("A request body must be provided containing the model information.");
				}
				createRequest.validate();
				String modelId = store.addModel(experimentName, createRequest);
				responder.sendString(GSON.toJson(new co.cask.mmds.manager.Id(modelId)));
			} catch (JsonParseException e) {
				throw new BadRequestException(String.format(
						"Problem occurred while parsing request to create model in experiment '%s'. " + "Error: %s",
						experimentName, e.getMessage()));
			}
		});
	}

	@PUT
	@Path("/experiments/{experiment-name}/models/{model-id}/directives")
	public void setModelDirectives(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName, @PathParam("model-id") final String modelId) {
		runInTx(responder, store -> {
			DirectivesRequest directives = GSON.fromJson(Bytes.toString(request.getContent()), DirectivesRequest.class);
			if (directives == null) {
				throw new BadRequestException("A request body must be provided containing the directives.");
			}
			directives.validate();
			store.setModelDirectives(new ModelKey(experimentName, modelId), directives.getDirectives());
			responder.sendStatus(200);
		});
	}

	@POST
	@Path("/experiments/{experiment-name}/models/{model-id}/split")
	public void createModelSplit(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName, @PathParam("model-id") final String modelId) {

		DataSplitInfo dataSplitInfo = callInTx(responder, store -> {
			try {
				ModelKey modelKey = new ModelKey(experimentName, modelId);
				DataSplit splitInfo = GSON.fromJson(Bytes.toString(request.getContent()), DataSplit.class);
				if (splitInfo == null) {
					throw new BadRequestException("A request body must be provided containing split parameters.");
				}
				// if no directives are given, use the ones from the model
				if (splitInfo.getDirectives().isEmpty()) {
					ModelMeta modelMeta = store.getModel(modelKey);
					splitInfo = new DataSplit(splitInfo.getDescription(), splitInfo.getType(), splitInfo.getParams(),
							modelMeta.getDirectives(), splitInfo.getSchema());
				}
				splitInfo.validate();
				DataSplitInfo info = store.addSplit(experimentName, splitInfo, System.currentTimeMillis());
				store.setModelSplit(modelKey, info.getSplitId());
				return info;
			} catch (JsonParseException e) {
				throw new BadRequestException(String.format(
						"Problem occurred while parsing request for split creation for experiment '%s'. " + "Error: %s",
						experimentName, e.getMessage()));
			} catch (IllegalArgumentException e) {
				throw new BadRequestException(e.getMessage());
			}
		});

		// happens if there was an error above
		if (dataSplitInfo == null) {
			return;
		}

		addSplit(dataSplitInfo);
		responder.sendStatus(200);
	}

	@DELETE
	@Path("/experiments/{experiment-name}/models/{model-id}/split")
	public void unassignModelSplit(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName, @PathParam("model-id") final String modelId) {
		runInTx(responder, store -> {
			store.unassignModelSplit(new ModelKey(experimentName, modelId));
			responder.sendStatus(200);
		});
	}

	@POST
	@Path("/experiments/{experiment-name}/models/{model-id}/train")
	public void trainModel(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName, @PathParam("model-id") final String modelId) {
		ModelTrainerInfo trainerInfo = callInTx(responder, store -> {
			try {
				TrainModelRequest trainRequest = GSON.fromJson(Bytes.toString(request.getContent()),
						TrainModelRequest.class);
				if (trainRequest == null) {
					throw new BadRequestException("A request body must be provided containing training parameters.");
				}
				trainRequest.validate();

				ModelKey modelKey = new ModelKey(experimentName, modelId);

				return store.trainModel(modelKey, trainRequest, System.currentTimeMillis());
			} catch (JsonParseException e) {
				throw new BadRequestException(String.format(
						"Problem occurred while parsing request for model training for experiment '%s'. " + "Error: %s",
						experimentName, e.getMessage()));
			}
		});

		// happens if there was an error above
		if (trainerInfo == null) {
			return;
		}

		ModelKey modelKey = new ModelKey(trainerInfo.getExperiment().getName(), trainerInfo.getModelId());

		new Thread(() -> {
			ModelLogging.start(modelKey.getExperiment(), modelKey.getModel());
			Schema schema = trainerInfo.getDataSplitStats().getSchema();
			ModelTrainer modelTrainer = new ModelTrainer(trainerInfo);
			StructType sparkSchema = DataFrames.toDataType(schema);
			try {
				// read training data
				Dataset<Row> rawTraining = sparkSession.read().format("parquet").schema(sparkSchema)
						.load(trainerInfo.getDataSplitStats().getTrainingPath());

				// read test data
				Dataset<Row> rawTest = sparkSession.read().format("parquet").schema(sparkSchema)
						.load(trainerInfo.getDataSplitStats().getTestPath());

				ModelOutput modelOutput = modelTrainer.train(rawTraining, rawTest);

				// write model components
				modelOutputWriter.save(modelKey, modelOutput, trainerInfo.getModel().getPredictionsDataset());

				// write model metadata
				runInTx(store -> store.updateModelMetrics(modelKey, modelOutput.getEvaluationMetrics(),
						System.currentTimeMillis(), modelOutput.getCategoricalFeatures()));
			} catch (Throwable e) {
				LOG.error("Error training model {} in experiment {}.", modelKey.getModel(), modelKey.getExperiment(),
						e);
				try {
					runInTx(store -> store.modelFailed(modelKey));
				} catch (TransactionFailureException te) {
					LOG.error("Error marking model {} in experiment {} as failed", modelKey.getModel(),
							modelKey.getExperiment(), te);
				}

				try {
					modelOutputWriter.deleteComponents(modelKey);
				} catch (IOException e1) {
					LOG.error("Error during cleanup after model {} in experiment {} failed to train.",
							modelKey.getModel(), modelKey.getExperiment(), e1);
				}
			} finally {
				ModelLogging.finish();
			}
		}).start();

		responder.sendStatus(200);
	}

	@DELETE
	@Path("/experiments/{experiment-name}/models/{model-id}")
	public void deleteModel(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("model-id") String modelId) {
		final ModelKey modelKey = new ModelKey(experimentName, modelId);
		runInTx(responder, store -> {
			store.deleteModel(modelKey);
			responder.sendStatus(200);
		});
	}

	@POST
	@Path("/experiments/{experiment-name}/models/{model-id}/deploy")
	public void deployModel(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("model-id") String modelId) {
		final ModelKey key = new ModelKey(experimentName, modelId);
		runInTx(responder, store -> {
			store.deployModel(key);
			responder.sendStatus(200);
		});
	}

	@GET
	@Path("/experiments/{experiment-name}/splits")
	public void listSplits(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName) {
		runInTx(responder, store -> responder.sendString(GSON.toJson(store.listSplits(experimentName))));
	}

	@POST
	@Path("/experiments/{experiment-name}/splits")
	public void addSplit(final HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") final String experimentName) {
		DataSplitInfo dataSplitInfo = callInTx(responder, store -> {
			try {
				DataSplit splitInfo = GSON.fromJson(Bytes.toString(request.getContent()), DataSplit.class);
				if (splitInfo == null) {
					throw new BadRequestException("A request body must be provided containing split parameters.");
				}
				splitInfo.validate();
				return store.addSplit(experimentName, splitInfo, System.currentTimeMillis());
			} catch (JsonParseException e) {
				throw new BadRequestException(String.format(
						"Problem occurred while parsing request for split creation for experiment '%s'. " + "Error: %s",
						experimentName, e.getMessage()));
			} catch (IllegalArgumentException e) {
				throw new BadRequestException(e.getMessage());
			}
		});

		// happens if there was an error above
		if (dataSplitInfo == null) {
			return;
		}

		addSplit(dataSplitInfo);

		responder.sendString(GSON.toJson(new co.cask.mmds.manager.Id(dataSplitInfo.getSplitId())));
	}

	@GET
	@Path("/experiments/{experiment-name}/splits/{split-id}")
	public void getSplit(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("split-id") String splitId) {
		final SplitKey key = new SplitKey(experimentName, splitId);
		runInTx(responder, store -> responder.sendString(GSON.toJson(store.getSplit(key))));
	}

	@GET
	@Path("/experiments/{experiment-name}/splits/{split-id}/status")
	public void getSplitStatus(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("split-id") String splitId) {
		final SplitKey key = new SplitKey(experimentName, splitId);
		runInTx(responder, store -> {
			DataSplitStats stats = store.getSplit(key);
			responder.sendString(GSON.toJson(stats.getStatus()));
		});
	}

	@DELETE
	@Path("/experiments/{experiment-name}/splits/{split-id}")
	public void deleteSplit(HttpServiceRequest request, HttpServiceResponder responder,
			@PathParam("experiment-name") String experimentName, @PathParam("split-id") String splitId) {
		final SplitKey key = new SplitKey(experimentName, splitId);
		runInTx(responder, store -> {
			store.deleteSplit(key);
			responder.sendStatus(200);
		});
	}

	
	@POST
	@Path("/pipeline/run")
	public void runPipeline(HttpServiceRequest request, HttpServiceResponder responder) {
		ETLBatchConfig config = null;
		BatchSparkPipelineDriver bspd = new BatchSparkPipelineDriver();
		SparkHttpServicePluginContext pluginContext = context.getPluginContext();
		BatchPhaseSpec batchPhaseSpec = null;

		//Get pipeline json from request
		try {
			config = GSON.fromJson(Bytes.toString(request.getContent()), ETLBatchConfig.class);
			if (config == null) {
				throw new BadRequestException("A request body must be provided the pipeline json to run.");
			}
		} catch (Exception e2) {
			e2.printStackTrace();
			throw new BadRequestException(String.format(
					"A request body must be provided the pipeline json to run '%s'. " + "Error: %s", e2.getMessage()));
		}

		//create batch pahse spec from input json
		try {

			BatchPipelineSpec spec = new InteractivePipelineSpecGenerator<SparkHttpServicePluginContext>(pluginContext,
					ImmutableSet.of(BatchSource.PLUGIN_TYPE),
					ImmutableSet.of(BatchSink.PLUGIN_TYPE, SparkSink.PLUGIN_TYPE, AlertPublisher.PLUGIN_TYPE),
					Engine.SPARK).generateSpec(config);

			int sourceCount = 0;
			for (StageSpec stageSpec : spec.getStages()) {
				if (BatchSource.PLUGIN_TYPE.equals(stageSpec.getPlugin().getType())) {
					sourceCount++;
				}
			}
			if (sourceCount != 1) {
				throw new IllegalArgumentException("Invalid pipeline. There must only be one source.");
			}

			PipelinePlanner planner = new PipelinePlanner(SUPPORTED_PLUGIN_TYPES, ImmutableSet.<String>of(),
					ImmutableSet.<String>of(), ImmutableSet.<String>of(), ImmutableSet.<String>of());
			PipelinePlan plan = planner.plan(spec);

			if (plan.getPhases().size() != 1) {
				// should never happen if there is only one source
				throw new IllegalArgumentException(
						"There was an error planning the pipeline. There should only be one phase.");
			}

			PipelinePhase pipeline = plan.getPhases().values().iterator().next();
			batchPhaseSpec = new BatchPhaseSpec(ETLSpark.class.getSimpleName(), pipeline, config.getResources(),
					config.getDriverResources(), config.getClientResources(), config.isStageLoggingEnabled(),
					config.isProcessTimingEnabled(), new HashMap<String, String>(), config.getNumOfRecordsPreview(),
					config.getProperties(), false);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new BadRequestException(String.format(
					"Problem in creating batchPhaseSpec from the given pipeline json '%s'. " + "Error: %s",
					ex.getMessage()));
		}

		/**
		 * Set spark pipeline json
		 */
		String json = GSON.toJson(batchPhaseSpec);
		Map<String, String> properties = context.getSpecification().getProperties();
		properties.put(Constants.PIPELINEID, json);

		try {

			List cleanupFiles = new ArrayList<>();
			List<Finisher> finishers = new ArrayList<>();
			SparkConf sparkConf = context.getSparkContext().getConf();

			BatchPhaseSpec phaseSpec = GSON.fromJson(properties.get(Constants.PIPELINEID), BatchPhaseSpec.class);

			for (Map.Entry<String, String> pipelineProperty : phaseSpec.getPipelineProperties().entrySet()) {
				sparkConf.set(pipelineProperty.getKey(), pipelineProperty.getValue());
			}

			MacroEvaluator evaluator = new DefaultMacroEvaluator(new BasicArguments(context.getRuntimeArguments()),
					context.getLogicalStartTime(), context.getSecureStore(), context.getNamespace());
			final SparkBatchSourceFactory sourceFactory = new SparkBatchSourceFactory();
			final SparkBatchSinkFactory sinkFactory = new SparkBatchSinkFactory();
			final Map<String, Integer> stagePartitions = new HashMap<>();
			/**
			 * PluginContext pluginContext = new
			 * SparkPipelinePluginContext(context.getPluginContext(), context.getMetrics(),
			 * phaseSpec.isStageLoggingEnabled(), phaseSpec.isProcessTimingEnabled());
			 * PipelinePluginInstantiator pluginInstantiator = new
			 * PipelinePluginInstantiator(context.getPluginContext(), context.getMetrics(),
			 * phaseSpec, new SingleConnectorFactory());
			 */

			final PipelineRuntime pipelineRuntime = new PipelineRuntime(context.getNamespace(), "debug_pipeline",
					context.getLogicalStartTime(), new BasicArguments(context.getRuntimeArguments()),
					context.getMetrics(), context.getPluginContext(), context.getServiceDiscoverer(), null, null, null);
			final Admin admin = context.getAdmin();

			PipelinePhase phase = phaseSpec.getPhase();
			// Collect field operations emitted by various stages in this MapReduce program
			final Map<String, List<FieldOperation>> stageOperations = new HashMap<>();
			// go through in topological order so that arguments set by one stage are seen
			// by stages after it
			for (final String stageName : phase.getDag().getTopologicalOrder()) {
				final StageSpec stageSpec = phase.getStage(stageName);
				String pluginType = stageSpec.getPluginType();
				boolean isConnectorSource = Constants.Connector.PLUGIN_TYPE.equals(pluginType)
						&& phase.getSources().contains(stageName);
				boolean isConnectorSink = Constants.Connector.PLUGIN_TYPE.equals(pluginType)
						&& phase.getSinks().contains(stageName);

				SubmitterPlugin submitterPlugin = null;
				if (BatchSource.PLUGIN_TYPE.equals(pluginType) || isConnectorSource) {

					BatchConfigurable<BatchSourceContext> batchSource = pluginContext.newPluginInstance(stageName,
							evaluator);
					ContextProvider<BatchSourceContext> contextProvider = new ContextProvider<BatchSourceContext>() {
						@Override
						public BatchSourceContext getContext(DatasetContext datasetContext) {
							return new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, datasetContext,
									stageSpec);
						}
					};
					submitterPlugin = new SubmitterPlugin(stageName, context, batchSource, contextProvider,
							new SubmitterPlugin.PrepareAction<SparkBatchSourceContext>() {
								@Override
								public void act(SparkBatchSourceContext context) {
									stageOperations.put(stageName, context.getFieldOperations());
								}
							});

				} else if (Transform.PLUGIN_TYPE.equals(pluginType)) {

					Transform transform = pluginContext.newPluginInstance(stageName, evaluator);
					ContextProvider<StageSubmitterContext> contextProvider = new ContextProvider<StageSubmitterContext>() {
						@Override
						public StageSubmitterContext getContext(DatasetContext datasetContext) {
							return new SparkBatchSourceContext(sourceFactory, context, pipelineRuntime, datasetContext,
									stageSpec);
						}
					};
					submitterPlugin = new SubmitterPlugin(stageName, context, transform, contextProvider,
							new SubmitterPlugin.PrepareAction<SparkBatchSourceContext>() {
								@Override
								public void act(SparkBatchSourceContext context) {
									stageOperations.put(stageName, context.getFieldOperations());
								}
							});

				} else if (BatchSink.PLUGIN_TYPE.equals(pluginType) || isConnectorSink) {

					BatchConfigurable<BatchSinkContext> batchSink = pluginContext.newPluginInstance(stageName,
							evaluator);
					ContextProvider<BatchSinkContext> contextProvider = new ContextProvider<BatchSinkContext>() {
						@Override
						public BatchSinkContext getContext(DatasetContext datasetContext) {
							return new SparkBatchSinkContext(sinkFactory, context, pipelineRuntime, datasetContext,
									stageSpec);
						}
					};
					submitterPlugin = new SubmitterPlugin(stageName, context, batchSink, contextProvider,
							new SubmitterPlugin.PrepareAction<SparkBatchSinkContext>() {
								@Override
								public void act(SparkBatchSinkContext context) {
									stageOperations.put(stageName, context.getFieldOperations());
								}
							});

				} else if (SparkSink.PLUGIN_TYPE.equals(pluginType)) {

					BatchConfigurable<SparkPluginContext> sparkSink = pluginContext.newPluginInstance(stageName,
							evaluator);
					ContextProvider<SparkPluginContext> contextProvider = new ContextProvider<SparkPluginContext>() {
						@Override
						public SparkPluginContext getContext(DatasetContext datasetContext) {
							return new BasicSparkPluginContext(null, pipelineRuntime, stageSpec, datasetContext, admin);
						}
					};
					submitterPlugin = new SubmitterPlugin(stageName, context, sparkSink, contextProvider);

				} else if (BatchAggregator.PLUGIN_TYPE.equals(pluginType)) {

					BatchAggregator aggregator = pluginContext.newPluginInstance(stageName, evaluator);
					ContextProvider<DefaultAggregatorContext> contextProvider = new AggregatorContextProvider(
							pipelineRuntime, stageSpec, admin);
					submitterPlugin = new SubmitterPlugin(stageName, context, aggregator, contextProvider,
							new SubmitterPlugin.PrepareAction<DefaultAggregatorContext>() {
								@Override
								public void act(DefaultAggregatorContext context) {
									stageOperations.put(stageName, context.getFieldOperations());
								}
							});

				} else if (BatchJoiner.PLUGIN_TYPE.equals(pluginType)) {

					BatchJoiner joiner = pluginContext.newPluginInstance(stageName, evaluator);
					ContextProvider<DefaultJoinerContext> contextProvider = new JoinerContextProvider(pipelineRuntime,
							stageSpec, admin);
					submitterPlugin = new SubmitterPlugin<>(stageName, context, joiner, contextProvider,
							new SubmitterPlugin.PrepareAction<DefaultJoinerContext>() {
								@Override
								public void act(DefaultJoinerContext sparkJoinerContext) {
									stagePartitions.put(stageName, sparkJoinerContext.getNumPartitions());
									stageOperations.put(stageName, sparkJoinerContext.getFieldOperations());
								}
							});

				}
				if (submitterPlugin != null) {
					submitterPlugin.prepareRun();
					finishers.add(submitterPlugin);
				}
			}

			SparkBatchSourceSinkFactoryInfo sourceSinkInfo = new SparkBatchSourceSinkFactoryInfo(sourceFactory,
					sinkFactory, stagePartitions);

			properties.put("HydratorSpark.config", GSON.toJson(sourceSinkInfo));

			/**
			 * WorkflowToken token = (WorkflowToken) context.getWorkflowToken(); if (token
			 * != null) { for (Map.Entry<String, String> entry :
			 * pipelineRuntime.getArguments().getAddedArguments().entrySet()) {
			 * token.put(entry.getKey(), entry.getValue()); } // Put the collected field
			 * operations in workflow token
			 * token.put(Constants.FIELD_OPERATION_KEY_IN_WORKFLOW_TOKEN,
			 * GSON.toJson(stageOperations)); }
			 */

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new BadRequestException(
					String.format("Problem in initilizing the plugins required for the pipeline '%s'. " + "Error: %s",
							ex.getMessage()));
		}

		// Run the pipeline in Batch spark piepeline driver
		try {
			JavaSparkExecutionContext jse = context.toJavaSparkExecutionContext();
			bspd.run(jse);
			responder.sendString("Pipeline ran succesfully!");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			throw new BadRequestException(String.format(
					"Problem occurred while running the pipeine in Batch Spark pipeline driver '%s'. " + "Error: %s",
					e1.getMessage()));
		}

	}
	
	/**
	 * Run some logic in a transaction.
	 */
	private void runInTx(final Consumer<ExperimentStore> consumer) throws TransactionFailureException {
		context.execute((datasetContext) -> {
			IndexedTable modelMeta = datasetContext.getDataset(modelMetaDataset);
			IndexedTable experiments = datasetContext.getDataset(experimentMetaDataset);
			PartitionedFileSet splits = datasetContext.getDataset(splitsDataset);
			ExperimentStore store = new ExperimentStore(new ExperimentMetaTable(experiments),
					new DataSplitTable(splits), new ModelTable(modelMeta));
			consumer.accept(store);
		});
	}

	/**
	 * Run some logic in a transaction, catching certain exceptions and responding
	 * with the relevant error code. Any EndpointException thrown by the consumer
	 * will be handled automatically.
	 */
	private void runInTx(final HttpServiceResponder responder, final Consumer<ExperimentStore> consumer) {
		try {
			context.execute((datasetContext) -> {
				IndexedTable modelMeta = datasetContext.getDataset(modelMetaDataset);
				IndexedTable experiments = datasetContext.getDataset(experimentMetaDataset);
				PartitionedFileSet splits = datasetContext.getDataset(splitsDataset);
				ExperimentStore store = new ExperimentStore(new ExperimentMetaTable(experiments),
						new DataSplitTable(splits), new ModelTable(modelMeta));
				try {
					consumer.accept(store);
				} catch (EndpointException e) {
					responder.sendError(e.getCode(), e.getMessage());
				}
			});
		} catch (TransactionFailureException e) {
			LOG.error("Transaction failure during service call", e);
			responder.sendError(500, e.getMessage());
		}
	}

	/**
	 * Run an endpoint method in a transaction, catching certain exceptions and
	 * responding with the relevant error code. Any EndpointException thrown by the
	 * function will be handled automatically.
	 */
	private <T> T callInTx(HttpServiceResponder responder, final Function<ExperimentStore, T> function) {
		AtomicReference<T> ref = new AtomicReference<>();
		runInTx(responder, store -> ref.set(function.apply(store)));
		return ref.get();
	}

	private void addSplit(DataSplitInfo dataSplitInfo) {
		String experimentName = dataSplitInfo.getExperiment().getName();
		String splitId = dataSplitInfo.getSplitId();
		SplitKey splitKey = new SplitKey(experimentName, splitId);
		new Thread(() -> {
			SplitLogging.start(experimentName, splitId);
			DatasetSplitter datasetSplitter = Splitters.getSplitter(dataSplitInfo.getDataSplit().getType());
			try (DataSplitStatsGenerator splitStatsGenerator = new DataSplitStatsGenerator(sparkSession,
					datasetSplitter, context.getPluginContext(), context.getServiceDiscoverer())) {
				DataSplitResult result = splitStatsGenerator.split(dataSplitInfo);
				runInTx(store -> store.finishSplit(splitKey, result.getTrainingPath(), result.getTestPath(),
						result.getStats(), System.currentTimeMillis()));
			} catch (Exception e) {
				LOG.error("Error generating split {} in experiment {}.", splitId, experimentName, e);
				try {
					runInTx(store -> store.splitFailed(splitKey, System.currentTimeMillis()));
				} catch (TransactionFailureException te) {
					LOG.error("Error marking split {} in experiment {} as failed", splitId, experimentName, te);
				}
			} finally {
				SplitLogging.finish();
			}
		}).start();
	}
}
