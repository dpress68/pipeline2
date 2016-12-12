package innovationlab.djp_tst2_0_1;

import routines.system.*;
import routines.system.SparkRunStat.*;
import routines.system.api.*;
import routines.Numeric;
import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.TalendString;
import routines.StringHandling;
import routines.Relational;
import routines.TalendDate;
import routines.Mathematical;
import routines.SQLike;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;
import java.io.ObjectInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.Date;

import org.apache.spark.mllib.linalg.Vector;

@SuppressWarnings("unused")
/**
 * Job: djp_tst2 Purpose: tst2<br>
 * Description:  <br>
 * Spark: SPARK_1_6 <br>
 * @author Pressman, Douglas
 * @version 6.2.1.20160704_1411
 * @status DEV
 */
public class djp_tst2 implements TalendJob {
	static {
		System.setProperty("TalendJob.log", "djp_tst2.log");
	}
	private final static String utf8Charset = "UTF-8";
	private GlobalVar globalMap = null;

	private static org.apache.log4j.Logger LOG = org.apache.log4j.Logger
			.getLogger(djp_tst2.class);
	// DI compatibility
	private static org.apache.log4j.Logger log = LOG;

	private static class GlobalVar {
		public static final String GLOBALVAR_PARAMS_PREFIX = "talend.globalvar.params.";
		private Configuration job;
		private java.util.Map<String, Object> map;

		public GlobalVar(Configuration job) {
			this.job = job;
			this.map = new java.util.HashMap<String, Object>();
		}

		public Object get(String key) {
			String tempValue = job.get(GLOBALVAR_PARAMS_PREFIX + key);
			if (tempValue != null) {
				return SerializationUtils.deserialize(Base64
						.decodeBase64(StringUtils.getBytesUtf8(tempValue)));
			} else {
				return null;
			}
		}

		public void put(String key, Object value) {
			if (value == null)
				return;
			job.set(GLOBALVAR_PARAMS_PREFIX + key, StringUtils
					.newStringUtf8(Base64.encodeBase64(SerializationUtils
							.serialize((Serializable) value))));
		}

		public void putLocal(String key, Object value) {
			map.put(key, value);
		}

		public Object getLocal(String key) {
			return map.get(key);
		}
	}

	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();

	public static class ContextProperties extends java.util.Properties {

		private static final long serialVersionUID = 1L;

		public static final String CONTEXT_FILE_NAME = "talend.context.fileName";
		public static final String CONTEXT_KEYS = "talend.context.keys";
		public static final String CONTEXT_PARAMS_PREFIX = "talend.context.params.";
		public static final String CONTEXT_PARENT_KEYS = "talend.context.parent.keys";
		public static final String CONTEXT_PARENT_PARAMS_PREFIX = "talend.context.parent.params.";

		public ContextProperties(java.util.Properties properties) {
			super(properties);
		}

		public ContextProperties() {
			super();
		}

		public ContextProperties(Configuration job) {
			super();
			String contextFileName = (String) job.get(CONTEXT_FILE_NAME);
			try {
				if (contextFileName != null && !"".equals(contextFileName)) {
					java.io.File contextFile = new java.io.File(contextFileName);
					if (contextFile.exists()) {
						java.io.InputStream contextIn = contextFile.toURI()
								.toURL().openStream();
						this.load(contextIn);
						contextIn.close();
					} else {
						java.io.InputStream contextIn = djp_tst2.class
								.getClassLoader().getResourceAsStream(
										"innovationlab/djp_tst2_0_1/contexts/"
												+ contextFileName);
						if (contextIn != null) {
							this.load(contextIn);
							contextIn.close();
						}
					}
				}
				Object contextKeys = job.get(CONTEXT_KEYS);
				if (contextKeys != null) {
					java.util.StringTokenizer st = new java.util.StringTokenizer(
							contextKeys.toString(), " ");
					while (st.hasMoreTokens()) {
						String contextKey = st.nextToken();
						if ((String) job
								.get(CONTEXT_PARAMS_PREFIX + contextKey) != null) {
							this.put(contextKey,
									job.get(CONTEXT_PARAMS_PREFIX + contextKey));
						}
					}
				}
				Object contextParentKeys = job.get(CONTEXT_PARENT_KEYS);
				if (contextParentKeys != null) {
					java.util.StringTokenizer st = new java.util.StringTokenizer(
							contextParentKeys.toString(), " ");
					while (st.hasMoreTokens()) {
						String contextKey = st.nextToken();
						if ((String) job.get(CONTEXT_PARENT_PARAMS_PREFIX
								+ contextKey) != null) {
							this.put(
									contextKey,
									job.get(CONTEXT_PARENT_PARAMS_PREFIX
											+ contextKey));
						}
					}
				}

				this.loadValue(null, job);
			} catch (java.io.IOException ie) {
				System.err.println("Could not load context " + contextFileName);
				ie.printStackTrace();
			}
		}

		public void synchronizeContext() {
		}

		public void loadValue(java.util.Properties context_param,
				Configuration job) {
		}
	}

	private ContextProperties context = new ContextProperties();

	public ContextProperties getContext() {
		return this.context;
	}

	public static class TalendRuntimeSparkListener extends
			org.apache.spark.ui.jobs.JobProgressListener {

		private boolean hasStarted = false;
		org.apache.spark.ui.jobs.UIData.JobUIData data;
		private java.util.HashMap<String, String> components;
		private java.util.List<String> alreadyComputedComponents;
		private java.util.ArrayList<String> cacheNodes;
		private int currentJobId = 0;
		private String currentComponentConnectionId = "";
		private String currentComponentName = "";
		private long startTimestamp = 0L;
		private long clientStartTimestamp = 0L;

		public TalendRuntimeSparkListener(org.apache.spark.SparkConf conf) {
			super(conf);
			this.components = new java.util.HashMap<>();
			this.components.put("tRedshiftOutput_1",
					"conntRedshiftOutput_1_1;push;row1;");
			this.components.put("tRedshiftOutput_1DummyMap_1", "push;row1;");
			this.components.put("tMap_1", "row1;");
			this.components.put("tRedshiftInput_1", "");
			this.components.put("tRedshiftConfiguration_1", "");
			this.components.put("tS3Configuration_1", "");
			this.components.put("tSparkConfiguration_1", "");
			this.cacheNodes = new java.util.ArrayList<>();
			this.alreadyComputedComponents = new java.util.ArrayList<>();
		}

		@Override
		public void onJobStart(
				org.apache.spark.scheduler.SparkListenerJobStart jobStart) {
			super.onJobStart(jobStart);

			this.currentComponentConnectionId = removeAlreadyUsedConnections(this.components
					.get(currentComponent));
			this.currentComponentName = currentComponent;
			this.currentJobId = jobStart.jobId();
			this.startTimestamp = jobStart.time();
			this.clientStartTimestamp = System.currentTimeMillis();

			log.info("The Spark job with the id <" + this.currentJobId
					+ "> has been launched.");
			log.debug("The Spark job with the id <"
					+ this.currentJobId
					+ "> will execute the transformations generated by this component: <"
					+ this.currentComponentName + ">.");

			hasStarted = true;
			java.util.Map<Object, org.apache.spark.ui.jobs.UIData.JobUIData> data = scala.collection.JavaConversions
					.asJavaMap(jobIdToData());
			this.data = data.get(this.currentJobId);
		}

		@Override
		public void onJobEnd(
				org.apache.spark.scheduler.SparkListenerJobEnd jobEnd) {
			super.onJobEnd(jobEnd);

			log.info(String.format(
					"The Spark job with the id <" + this.currentJobId
							+ "> has been finished - Duration: %s.",
					org.apache.commons.lang3.time.DurationFormatUtils
							.formatDurationHMS(jobEnd.time()
									- this.startTimestamp)));
			log.debug("The Spark job with the id <"
					+ this.currentJobId
					+ "> has executed the transformations generated by this component: <"
					+ this.currentComponentName + ">.");
		}

		@Override
		public void onExecutorMetricsUpdate(
				org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
			super.onExecutorMetricsUpdate(executorMetricsUpdate);
			if (hasStarted) {

				log.info(String
						.format("The Spark job with the id <"
								+ this.currentJobId
								+ "> is still in progress... Elapsed time: %s.",
								org.apache.commons.lang3.time.DurationFormatUtils
										.formatDurationHMS(System
												.currentTimeMillis()
												- this.clientStartTimestamp)));
				log.trace("Spark job: <" + this.currentJobId
						+ "> - Number of tasks: <" + data.numTasks()
						+ "> - Number of completed tasks: <"
						+ data.numCompletedTasks()
						+ "> - Number of skipped tasks: <"
						+ data.numSkippedTasks() + ">.");
			}
		}

		private String removeAlreadyUsedConnections(String connectionId) {
			String localConnectionId = connectionId;
			for (String component : alreadyComputedComponents) {
				String alreadyComputedComponentConnectionId = this.components
						.get(component);
				if (localConnectionId
						.contains(alreadyComputedComponentConnectionId)) {
					localConnectionId = localConnectionId.replace(
							alreadyComputedComponentConnectionId, "");
				}
			}

			java.util.ArrayList<String> cacheNodesClone = (ArrayList<String>) this.cacheNodes
					.clone();
			for (String cacheNode : cacheNodesClone) {
				String cacheNodeConnectionId = this.components.get(cacheNode);
				if (connectionId.contains(cacheNodeConnectionId)) {
					alreadyComputedComponents.add(0, cacheNode);
					this.cacheNodes.remove(cacheNode);
				}
			}
			alreadyComputedComponents.add(0, currentComponent);
			return localConnectionId;
		}

	}

	private static SparkRunStat runStat = new SparkRunStat();

	public static class TalendEndOfRunSparkListener extends
			org.apache.spark.JavaSparkListener {

		public TalendEndOfRunSparkListener(String appName) {
			// onApplicationStart is called only with spark-submit
			this.appName = appName;
			appStartTime = System.currentTimeMillis();
		}

		@Override
		public void onApplicationEnd(
				org.apache.spark.scheduler.SparkListenerApplicationEnd applicationEnd) {
			long appDuration = applicationEnd.time() - appStartTime;
			log.info(String.format("Application %s ended in %s", appName,
					org.apache.commons.lang3.time.DurationFormatUtils
							.formatDurationHMS(appDuration)));
			log.info("Spark counters per job (" + metricsPerJob.size()
					+ " jobs):");
			java.text.DecimalFormat df = new java.text.DecimalFormat("#.####");

			long totalJobDuration = 0;

			for (EndOfRunMetrics metrics : metricsPerJob.values()) {
				long jobDuration = metrics.duration();
				totalJobDuration += jobDuration;
				log.info(String.format("%s.job%d.duration=%s", appName,
						metrics.jobId,
						org.apache.commons.lang3.time.DurationFormatUtils
								.formatDurationHMS(jobDuration)));
				log.info(String.format("%s.job%d.recordsRead=%d", appName,
						metrics.jobId, metrics.recordsRead));
				log.info(String.format("%s.job%d.shuffleRecordsRead=%d",
						appName, metrics.jobId, metrics.shuffleRecordsRead));
				log.info(String.format("%s.job%d.shuffleRecordsWritten=%d",
						appName, metrics.jobId, metrics.shuffleRecordsWritten));
				log.info(String.format("%s.job%d.recordsWritten=%d", appName,
						metrics.jobId, metrics.recordsWritten));
				log.info(String.format("%s.job%d.bytesRead=%d", appName,
						metrics.jobId, metrics.bytesRead));
				log.info(String.format("%s.job%d.shuffleBytesRead=%d", appName,
						metrics.jobId, metrics.shuffleBytesRead));
				log.info(String.format("%s.job%d.shuffleBytesWritten=%d",
						appName, metrics.jobId, metrics.shuffleBytesWritten));
				log.info(String.format("%s.job%d.bytesWritten=%d", appName,
						metrics.jobId, metrics.bytesWritten));
				log.info(String.format("%s.job%d.diskBytesSpilled=%d", appName,
						metrics.jobId, metrics.diskBytesSpilled));
				log.info(String.format("%s.job%d.memoryBytesSpilled=%d",
						appName, metrics.jobId, metrics.memoryBytesSpilled));
				log.info(String.format("%s.job%d.shuffleTotalBlocksFetched=%d",
						appName, metrics.jobId,
						metrics.shuffleTotalBlocksFetched));
				log.info(String.format("%s.job%d.jvmGcTime=%s", appName,
						metrics.jobId,
						org.apache.commons.lang3.time.DurationFormatUtils
								.formatDurationHMS(metrics.jvmGCTime)));
				log.info(String.format(
						"%s.job%d.pctTimeSpentInGC=%s",
						appName,
						metrics.jobId,
						df.format(jobDuration > 0 ? metrics.jvmGCTime
								/ new Double(jobDuration) : 0.)));
			}
			log.info("Total Spark counters :");
			log.info(String.format(
					"%s.total.executors(min/max/avg)=%d/%d/%s",
					appName,
					minExecutors,
					maxExecutors,
					df.format(countUpdates > 0 ? totalNbExecutors
							/ new Double(countUpdates) : 0.)));

			EndOfRunMetrics metrics = totalAggregation();
			log.info(String.format("%s.total.recordsRead=%d", appName,
					metrics.recordsRead));
			log.info(String.format("%s.total.shuffleRecordsRead=%d", appName,
					metrics.shuffleRecordsRead));
			log.info(String.format("%s.total.shuffleRecordsWritten=%d",
					appName, metrics.shuffleRecordsWritten));
			log.info(String.format("%s.total.recordsWritten=%d", appName,
					metrics.recordsWritten));
			log.info(String.format("%s.total.bytesRead=%d", appName,
					metrics.bytesRead));
			log.info(String.format("%s.total.shuffleBytesRead=%d", appName,
					metrics.shuffleBytesRead));
			log.info(String.format("%s.total.shuffleBytesWritten=%d", appName,
					metrics.shuffleBytesWritten));
			log.info(String.format("%s.total.bytesWritten=%d", appName,
					metrics.bytesWritten));
			log.info(String.format("%s.total.diskBytesSpilled=%d", appName,
					metrics.diskBytesSpilled));
			log.info(String.format("%s.total.memoryBytesSpilled=%d", appName,
					metrics.memoryBytesSpilled));
			log.info(String.format("%s.total.shuffleTotalBlocksFetched=%d",
					appName, metrics.shuffleTotalBlocksFetched));
			log.info(String.format("%s.total.jvmGcTime=%s", appName,
					org.apache.commons.lang3.time.DurationFormatUtils
							.formatDurationHMS(metrics.jvmGCTime)));
			log.info(String.format(
					"%s.total.pctTimeSpentInGC=%s",
					appName,
					df.format(totalJobDuration > 0 ? metrics.jvmGCTime
							/ new Double(totalJobDuration) : 0.)));
		}

		@Override
		public void onJobStart(
				org.apache.spark.scheduler.SparkListenerJobStart jobStart) {
			java.util.List<Object> ids = scala.collection.JavaConversions
					.asJavaList(jobStart.stageIds());
			EndOfRunMetrics newJob = new EndOfRunMetrics(jobStart.jobId(),
					jobStart.time(), ids);
			metricsPerJob.put(newJob.jobId, newJob);
		}

		@Override
		public void onJobEnd(
				org.apache.spark.scheduler.SparkListenerJobEnd jobEnd) {
			EndOfRunMetrics oldJob = metricsPerJob.get(jobEnd.jobId());
			if (oldJob != null) {
				oldJob.endTime = jobEnd.time();
			}
		}

		@Override
		public void onTaskEnd(
				org.apache.spark.scheduler.SparkListenerTaskEnd taskEnd) {
			if (taskEnd.taskInfo() == null
					|| !"SUCCESS".equals(taskEnd.taskInfo().status()))
				return;
			countUpdates++;
			totalNbExecutors += currentExecutors;
			Integer stageId = taskEnd.stageId();
			for (EndOfRunMetrics jobMetrics : metricsPerJob.values()) {
				if (jobMetrics.endTime != null)
					continue;
				for (Object id : jobMetrics.stageIds) {
					if (!id.equals(stageId))
						continue;
					org.apache.spark.executor.TaskMetrics metrics = taskEnd
							.taskMetrics();

					if (metrics == null) {
						continue;
					}

					jobMetrics.diskBytesSpilled += metrics.diskBytesSpilled();
					jobMetrics.jvmGCTime += metrics.jvmGCTime();
					jobMetrics.memoryBytesSpilled += metrics
							.memoryBytesSpilled();

					if (metrics.inputMetrics().isDefined()) {
						org.apache.spark.executor.InputMetrics inputMetrics = metrics
								.inputMetrics().get();
						jobMetrics.bytesRead += inputMetrics.bytesRead();
						jobMetrics.recordsRead += inputMetrics.recordsRead();
					}

					if (metrics.outputMetrics().isDefined()) {
						org.apache.spark.executor.OutputMetrics outputMetrics = metrics
								.outputMetrics().get();
						jobMetrics.bytesWritten += outputMetrics.bytesWritten();
						jobMetrics.recordsWritten += outputMetrics
								.recordsWritten();
					}

					if (metrics.shuffleReadMetrics().isDefined()) {
						org.apache.spark.executor.ShuffleReadMetrics srMetrics = metrics
								.shuffleReadMetrics().get();
						jobMetrics.shuffleRecordsRead += srMetrics
								.recordsRead();
						jobMetrics.shuffleBytesRead += srMetrics
								.totalBytesRead();
						jobMetrics.shuffleTotalBlocksFetched += srMetrics
								.totalBlocksFetched();
					}

					if (metrics.shuffleWriteMetrics().isDefined()) {
						org.apache.spark.executor.ShuffleWriteMetrics swMetrics = metrics
								.shuffleWriteMetrics().get();
						jobMetrics.shuffleRecordsWritten += swMetrics
								.shuffleRecordsWritten();
						jobMetrics.shuffleBytesWritten += swMetrics
								.shuffleBytesWritten();
					}

					return;
				}
			}
		}

		@Override
		public void onExecutorAdded(
				org.apache.spark.scheduler.SparkListenerExecutorAdded executorAdded) {
			currentExecutors++;
			if (currentExecutors > maxExecutors)
				maxExecutors = currentExecutors;
		}

		@Override
		public void onExecutorRemoved(
				org.apache.spark.scheduler.SparkListenerExecutorRemoved executorRemoved) {
			currentExecutors--;
			if (currentExecutors < minExecutors)
				minExecutors = currentExecutors;
		}

		private EndOfRunMetrics totalAggregation() {
			EndOfRunMetrics total = new EndOfRunMetrics(0, 0, null);
			for (EndOfRunMetrics jobMetrics : metricsPerJob.values()) {
				total.diskBytesSpilled += jobMetrics.diskBytesSpilled;
				total.jvmGCTime += jobMetrics.jvmGCTime;
				total.memoryBytesSpilled += jobMetrics.memoryBytesSpilled;
				total.bytesRead += jobMetrics.bytesRead;
				total.recordsRead += jobMetrics.recordsRead;
				total.bytesWritten += jobMetrics.bytesWritten;
				total.recordsWritten += jobMetrics.recordsWritten;
				total.shuffleRecordsRead += jobMetrics.shuffleRecordsRead;
				total.shuffleBytesRead += jobMetrics.shuffleBytesRead;
				total.shuffleTotalBlocksFetched += jobMetrics.shuffleTotalBlocksFetched;
				total.shuffleRecordsWritten += jobMetrics.shuffleRecordsWritten;
				total.shuffleBytesWritten += jobMetrics.shuffleBytesWritten;
			}
			return total;
		}

		private long appStartTime = 0;
		private long minExecutors = 0;
		private long maxExecutors = 0;
		private long currentExecutors = 0;
		private long totalNbExecutors = 0;
		private long countUpdates = 0;
		private String appName = null;

		private java.util.Map<Integer, EndOfRunMetrics> metricsPerJob = new java.util.HashMap<>();

	}

	public static class EndOfRunMetrics {
		public int nbTasks = 0;
		public Integer jobId = null;
		public java.util.List<Object> stageIds = null;
		public Long startTime = null;
		public Long endTime = null;
		public Long diskBytesSpilled = 0L;
		public Long jvmGCTime = 0L;
		public Long memoryBytesSpilled = 0L;
		public Long bytesRead = 0L;
		public Long recordsRead = 0L;
		public Long bytesWritten = 0L;
		public Long recordsWritten = 0L;
		public Long shuffleRecordsRead = 0L;
		public Long shuffleBytesRead = 0L;
		public Long shuffleTotalBlocksFetched = 0L;
		public Long shuffleRecordsWritten = 0L;
		public Long shuffleBytesWritten = 0L;

		public EndOfRunMetrics(int jobId, long startTime,
				java.util.List<Object> stageIds) {
			this.jobId = jobId;
			this.startTime = startTime;
			this.stageIds = stageIds;
		}

		public long duration() {
			if (endTime != null && startTime != null)
				return endTime - startTime;
			return 0;
		}

	}

	private final static String jobVersion = "0.1";
	private final static String jobName = "djp_tst2";
	private final static String projectName = "INNOVATIONLAB";
	public Integer errorCode = null;

	private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
	private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(
			new java.io.BufferedOutputStream(baos));

	private static String currentComponent = "";

	public String getExceptionStackTrace() {
		if ("failure".equals(this.getStatus())) {
			errorMessagePS.flush();
			return baos.toString();
		}
		return null;
	}

	// should be remove later
	public void setDataSources(
			java.util.Map<String, javax.sql.DataSource> dataSources) {

	}

	/**
	 * [tRedshiftInput_1 sparkcode ] start
	 */
	public static class tRedshiftInput_1_FromRowTorow1Struct
			implements
			org.apache.spark.api.java.function.Function<org.apache.spark.sql.Row, row1Struct> {
		java.util.Set<String> fieldsToTrim = new java.util.HashSet<String>();

		public tRedshiftInput_1_FromRowTorow1Struct() {
		}

		public row1Struct call(org.apache.spark.sql.Row row) {
			row1Struct result = new row1Struct();
			org.apache.spark.sql.types.StructField[] structFields = row
					.schema().fields();
			for (int i = 0; i < structFields.length; i++) {
				org.apache.avro.Schema.Field avroField = row1Struct
						.getClassSchema().getField(structFields[i].name());
				if (avroField != null) {
					org.apache.avro.Schema fieldSchema = avroField.schema();
					String talendType = getTalendType(fieldSchema);
					boolean trim = false || fieldsToTrim
							.contains(structFields[i].name());
					try {
						result.put(
								avroField.pos(),
								handleNull(
										convertValue(row.get(i),
												structFields[i].dataType(),
												talendType, trim), talendType,
										isNullable(fieldSchema), null));
					} catch (Exception e) {
						throw new RuntimeException("col: "
								+ structFields[i].name() + " - sparkSQL type: "
								+ structFields[i].dataType()
								+ " - talend type:" + talendType + " - "
								+ e.getMessage());
					}
				}
			}
			return result;
		}

		private String getTalendType(org.apache.avro.Schema fieldSchema) {
			String talendType = null;
			if (org.apache.avro.Schema.Type.UNION.equals(fieldSchema.getType())) {
				for (org.apache.avro.Schema childFieldSchema : fieldSchema
						.getTypes()) {
					if (!org.apache.avro.Schema.Type.NULL
							.equals(childFieldSchema.getType())) {
						if (childFieldSchema.getProp("java-class") != null) {
							talendType = childFieldSchema.getProp("java-class");
						} else {
							talendType = childFieldSchema.getType().getName();
						}
					}
				}
			} else {
				if (fieldSchema.getProp("java-class") != null) {
					talendType = fieldSchema.getProp("java-class");
				} else {
					talendType = fieldSchema.getType().getName();
				}
			}
			return talendType;
		}

		private boolean isNullable(org.apache.avro.Schema fieldSchema) {
			boolean nullable = false;
			if (org.apache.avro.Schema.Type.UNION.equals(fieldSchema.getType())) {
				for (org.apache.avro.Schema childFieldSchema : fieldSchema
						.getTypes()) {
					if (org.apache.avro.Schema.Type.NULL
							.equals(childFieldSchema.getType())) {
						nullable = true;
						break;
					}
				}
			}
			return nullable;
		}

		private Object convertValue(Object value,
				org.apache.spark.sql.types.DataType sparkSqlType,
				String destType, boolean trim) {
			if (value != null) {
				if (sparkSqlType
						.equals(org.apache.spark.sql.types.DataTypes.StringType)) {
					// trim
					value = trim ? ((String) value).trim() : (String) value;
				} else if (sparkSqlType
						.equals(org.apache.spark.sql.types.DataTypes.TimestampType)) {
					// convert java.sql.Timestamp to java.util.Date
					value = new java.util.Date(
							((java.sql.Timestamp) value).getTime());
				} else if (sparkSqlType
						.equals(org.apache.spark.sql.types.DataTypes.DateType)) {
					// convert java.sql.Date to java.util.Date
					value = new java.util.Date(
							((java.sql.Date) value).getTime());
				} else if (sparkSqlType
						.equals(org.apache.spark.sql.types.DataTypes.BinaryType)) {
					value = java.nio.ByteBuffer.wrap((byte[]) value);
				}
			}
			String sourceType = sparkSqlTypeToTalendType.get(sparkSqlType
					.getClass());
			return convertValue(value, sourceType, destType);
		}

		private Object convertValue(Object value, String sourceType,
				String destType) {
			if (value == null || sourceType.equals(destType)
					|| OBJECT.equals(destType)) {
				return value;
			}
			// TODO consider all the situation between sourceType and destType
			if (BYTE.equals(destType)) {
				if (INT.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Byte((Integer) value);
				} else if (LONG.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Byte((Long) value);
				} else if (BOOLEAN.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Byte((Boolean) value);
				}
			} else if (CHAR.equals(destType)) {
				if (STRING.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Character((String) value);
				} else if (BYTE.equals(sourceType) || BYTES.equals(sourceType)) {
					return BigDataParserUtils
							.parseTo_Character(BigDataParserUtils
									.parseTo_String((java.nio.ByteBuffer) value));
				}
			} else if (INT.equals(destType)) {
				if (DECIMAL.equals(sourceType)) {
					return ((java.math.BigDecimal) value).intValue();
				}
			} else if (SHORT.equals(destType)) {
				if (INT.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Short((Integer) value);
				} else if (LONG.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Short((Long) value);
				} else if (DECIMAL.equals(sourceType)) {
					return ((java.math.BigDecimal) value).shortValueExact();
				}
			} else if (LONG.equals(destType)) {
				if (DECIMAL.equals(sourceType)) {
					return ((java.math.BigDecimal) value).longValue();
				}
			} else if (FLOAT.equals(destType)) {
				if (DOUBLE.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Float((Double) value);
				} else if (DECIMAL.equals(sourceType)) {
					return ((java.math.BigDecimal) value).floatValue();
				}
			} else if (DOUBLE.equals(destType)) {
				if (FLOAT.equals(sourceType)) {
					return BigDataParserUtils.parseTo_Double((Float) value);
				} else if (DECIMAL.equals(sourceType)) {
					return ((java.math.BigDecimal) value).doubleValue();
				}
			} else if (DECIMAL.equals(destType)) {
				if (INT.equals(sourceType)) {
					return BigDataParserUtils
							.parseTo_BigDecimal((Integer) value);
				} else if (LONG.equals(sourceType)) {
					return BigDataParserUtils.parseTo_BigDecimal((Long) value);
				} else if (FLOAT.equals(sourceType)) {
					return BigDataParserUtils.parseTo_BigDecimal((Float) value);
				} else if (DOUBLE.equals(sourceType)) {
					return BigDataParserUtils
							.parseTo_BigDecimal((Double) value);
				}
			}
			return value;
		}

		private Object handleNull(Object value, String destType,
				boolean nullable, Object defaultValue) {
			if (nullable) {
				return value;
			} else {
				if (value == null) {
					if (defaultValue != null) {
						return defaultValue;
					} else {
						switch (destType) {
						case BYTE:
						case SHORT:
						case INT:
						case LONG:
						case FLOAT:
						case DOUBLE:
							value = 0;
							break;
						case BOOLEAN:
							value = false;
							break;
						case DECIMAL:
						case STRING:
						case BYTES:
						case DATE:
						case LIST:
							break;
						default:
							break;
						}
						return value;
					}
				} else {
					return value;
				}
			}
		}

		private static final String BYTE = "java.lang.Byte";
		private static final String SHORT = "java.lang.Short";
		private static final String INT = "int";
		private static final String LONG = "long";
		private static final String FLOAT = "float";
		private static final String DOUBLE = "double";
		private static final String DECIMAL = "java.math.BigDecimal";
		private static final String STRING = "java.lang.String";
		private static final String BYTES = "bytes";
		private static final String BOOLEAN = "boolean";
		private static final String DATE = "java.util.Date";
		private static final String LIST = "java.util.List";
		private static final String OBJECT = "java.lang.Object";
		private static final String CHAR = "java.lang.Character";

		private static Map<Class<? extends org.apache.spark.sql.types.DataType>, String> sparkSqlTypeToTalendType = new java.util.HashMap<Class<? extends org.apache.spark.sql.types.DataType>, String>();

		static {
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.ByteType.getClass(),
					BYTE);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.ShortType.getClass(),
					SHORT);
			sparkSqlTypeToTalendType
					.put(org.apache.spark.sql.types.DataTypes.IntegerType
							.getClass(), INT);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.LongType.getClass(),
					LONG);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.FloatType.getClass(),
					FLOAT);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.DoubleType.getClass(),
					DOUBLE);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DecimalType.class, DECIMAL);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.StringType.getClass(),
					STRING);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.BinaryType.getClass(),
					BYTES);
			sparkSqlTypeToTalendType
					.put(org.apache.spark.sql.types.DataTypes.BooleanType
							.getClass(), BOOLEAN);
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.TimestampType
							.getClass(), DATE);// there is conversation from
												// java.sql.timestamp to
												// java.util.date
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.DataTypes.DateType.getClass(),
					DATE);// there is conversation from java.sql.date to
							// java.util.date
			sparkSqlTypeToTalendType.put(
					org.apache.spark.sql.types.ArrayType.class, LIST);

			// ignore MapType/StructType/StructField
		}
	}

	/**
	 * [tRedshiftInput_1 sparkcode ] stop
	 */
	/**
	 * [tMap_1 sparkcode ] start
	 */

	/**
	 * [tMap_1 sparkcode ] stop
	 */
	/**
	 * [tRedshiftOutput_1DummyMap_1 sparkcode ] start
	 */
	/**
	 * [tRedshiftOutput_1DummyMap_1 sparkcode ] stop
	 */
	/**
	 * [tRedshiftOutput_1 sparkcode ] start
	 */
	/**
	 * [tRedshiftOutput_1 sparkcode ] stop
	 */
	public void tRedshiftInput_1Process(
			final org.apache.spark.api.java.JavaSparkContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
		final JobConf job = new JobConf(ctx.hadoopConfiguration());

		try {
			/**
			 * [tRedshiftInput_1 sparkconfig ] start
			 */
			currentComponent = "tRedshiftInput_1";

			StringBuilder log4jParamters_tRedshiftInput_1 = new StringBuilder();
			log4jParamters_tRedshiftInput_1.append("Parameters:");
			log4jParamters_tRedshiftInput_1.append("USE_EXISTING_CONNECTION"
					+ " = " + "false");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1
					.append("HOST"
							+ " = "
							+ "\"mcdtalendpoc.cj4pclpbavnm.us-west-2.redshift.amazonaws.com\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("PORT" + " = " + "\"5439\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("USER" + " = "
					+ "\"slalommcd\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("PASS"
					+ " = "
					+ String.valueOf("e30b2b53a2a02395aa4cf535bb233d37")
							.substring(0, 4) + "...");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("DBNAME" + " = "
					+ "\"mcdtalendpoc\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("SCHEMA_DB" + " = "
					+ "\"public\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("PROPERTIES" + " = "
					+ "\"\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("S3_CONFIGURATION" + " = "
					+ "tS3Configuration_1");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("S3_TEMP_PATH" + " = "
					+ "\"/tmp\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("TABLE" + " = "
					+ "\"emp_id\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("READ_MODE" + " = "
					+ "QUERY");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("QUERYSTORE" + " = "
					+ "\"\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("QUERY" + " = "
					+ "\"select id, name from emp_id\"");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("TRIM_ALL_COLUMN" + " = "
					+ "false");
			log4jParamters_tRedshiftInput_1.append(" | ");
			log4jParamters_tRedshiftInput_1.append("TRIM_COLUMN" + " = "
					+ "[{TRIM=" + ("false") + ", SCHEMA_COLUMN=" + ("name")
					+ "}, {TRIM=" + ("false") + ", SCHEMA_COLUMN=" + ("id")
					+ "}]");
			log4jParamters_tRedshiftInput_1.append(" | ");
			if (log.isDebugEnabled())
				log.debug("tRedshiftInput_1 - "
						+ log4jParamters_tRedshiftInput_1);
			ctx.hadoopConfiguration().set("fs.s3a.access.key",
					"AKIAIAK6HDAVEP52ZYEA");
			ctx.hadoopConfiguration()
					.set("fs.s3a.secret.key",
							routines.system.PasswordEncryptUtil
									.decryptPassword("4c869c8ce88cb031a30a9a41d9f1de09b06214e4c24d61dd311ef92c21f40fc7f9942e83cbee5e79f4f7aba1746784ea"));
			org.apache.spark.sql.SQLContext sqlCtx_tRedshiftInput_1 = new org.apache.spark.sql.SQLContext(
					ctx);
			org.apache.spark.sql.DataFrame df_tRedshiftInput_1 = sqlCtx_tRedshiftInput_1
					.read()
					.format("com.databricks.spark.redshift")
					.option("url",
							"jdbc:redshift://"
									+ "mcdtalendpoc.cj4pclpbavnm.us-west-2.redshift.amazonaws.com"
									+ ":"
									+ "5439"
									+ "/"
									+ "mcdtalendpoc"
									+ "?user="
									+ "slalommcd"
									+ "&password="
									+ routines.system.PasswordEncryptUtil
											.decryptPassword("e30b2b53a2a02395aa4cf535bb233d37")
									+ "")

					.option("query", "select id, name from emp_id")

					.option("tempdir", "s3a://" + "dougpressman" + "/tmp")
					.load();

			// Retrieve the associated RDD
			org.apache.spark.api.java.JavaRDD<row1Struct> rdd_row1 = df_tRedshiftInput_1
					.toJavaRDD()
					.map(new tRedshiftInput_1_FromRowTorow1Struct());
			/**
			 * [tRedshiftInput_1 sparkconfig ] stop
			 */
			/**
			 * [tMap_1 sparkconfig ] start
			 */
			currentComponent = "tMap_1";

			StringBuilder log4jParamters_tMap_1 = new StringBuilder();
			log4jParamters_tMap_1.append("Parameters:");
			log4jParamters_tMap_1.append("LINK_STYLE" + " = " + "AUTO");
			log4jParamters_tMap_1.append(" | ");
			log4jParamters_tMap_1.append("REPLICATED_JOIN" + " = " + "false");
			log4jParamters_tMap_1.append(" | ");
			if (log.isDebugEnabled())
				log.debug("tMap_1 - " + log4jParamters_tMap_1);
			org.apache.spark.api.java.JavaPairRDD<NullWritable, row1Struct> rdd_push;
			{
				// Set up a Spark DataFlow with all of the input datasets
				// necessary to use this component.
				org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlowContext sdfContext = new org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlowContext.Builder()
						.withSparkContext(ctx).build();
				org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlow sdf = new org.talend.bigdata.dataflow.spark.batch.SparkBatchDataFlow(
						sdfContext);
				sdf.put("row1", rdd_row1);

				// Set up a component to perform the map.
				org.talend.bigdata.dataflow.hmap.HMap hmap = new org.talend.bigdata.dataflow.hmap.HMap();
				org.talend.bigdata.dataflow.hmap.HMapSpecBuilder hsb = hmap
						.createSpecBuilder();
				hsb.declareInput("row1", row1Struct.getClassSchema());
				hsb.declareOutput("push", row1Struct.getClassSchema());
				hsb.map("row1.name", "push.name");
				hsb.map("row1.id", "push.id");
				hmap.createDataFlowBuilder(hsb.build()).build(sdf);

				rdd_push = sdf.getPairRDD("push");
			}
			/**
			 * [tMap_1 sparkconfig ] stop
			 */
			/**
			 * [tRedshiftOutput_1DummyMap_1 sparkconfig ] start
			 */
			currentComponent = "tRedshiftOutput_1DummyMap_1";

			org.apache.spark.api.java.JavaRDD<row1Struct> rdd_conntRedshiftOutput_1_1 = rdd_push
					.values();
			/**
			 * [tRedshiftOutput_1DummyMap_1 sparkconfig ] stop
			 */
			/**
			 * [tRedshiftOutput_1 sparkconfig ] start
			 */
			currentComponent = "tRedshiftOutput_1";

			StringBuilder log4jParamters_tRedshiftOutput_1 = new StringBuilder();
			log4jParamters_tRedshiftOutput_1.append("Parameters:");
			log4jParamters_tRedshiftOutput_1.append("USE_EXISTING_CONNECTION"
					+ " = " + "false");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1
					.append("HOST"
							+ " = "
							+ "\"mcdtalendpoc.cj4pclpbavnm.us-west-2.redshift.amazonaws.com\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1
					.append("PORT" + " = " + "\"5439\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("USER" + " = "
					+ "\"slalommcd\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("PASS"
					+ " = "
					+ String.valueOf("e30b2b53a2a02395aa4cf535bb233d37")
							.substring(0, 4) + "...");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DBNAME" + " = "
					+ "\"mcdtalendpoc\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("SCHEMA_DB" + " = "
					+ "\"public\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("PROPERTIES" + " = "
					+ "\"\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("S3_CONFIGURATION" + " = "
					+ "tS3Configuration_1");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("S3_TEMP_PATH" + " = "
					+ "\"/tmp\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("TABLE" + " = "
					+ "\"djp_emp_id\"");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("SAVEMODE" + " = "
					+ "Append");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DIST_STYLE" + " = "
					+ "EVEN");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DEFINE_SORT_KEY" + " = "
					+ "false");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DEFINE_PRE_ACTIONS"
					+ " = " + "false");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DEFINE_POST_ACTIONS"
					+ " = " + "false");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			log4jParamters_tRedshiftOutput_1.append("DEFINE_EXTRA_COPY_OPTIONS"
					+ " = " + "false");
			log4jParamters_tRedshiftOutput_1.append(" | ");
			if (log.isDebugEnabled())
				log.debug("tRedshiftOutput_1 - "
						+ log4jParamters_tRedshiftOutput_1);
			ctx.hadoopConfiguration().set("fs.s3a.access.key",
					"AKIAIAK6HDAVEP52ZYEA");
			ctx.hadoopConfiguration()
					.set("fs.s3a.secret.key",
							routines.system.PasswordEncryptUtil
									.decryptPassword("4c869c8ce88cb031a30a9a41d9f1de09b06214e4c24d61dd311ef92c21f40fc7f9942e83cbee5e79f4f7aba1746784ea"));
			org.apache.spark.sql.SQLContext sqlCtx_tRedshiftOutput_1 = new org.apache.spark.sql.SQLContext(
					ctx);

			org.apache.spark.sql.DataFrame df_tRedshiftOutput_1_conntRedshiftOutput_1_1 = sqlCtx_tRedshiftOutput_1
					.createDataFrame(rdd_conntRedshiftOutput_1_1,
							row1Struct.class);

			df_tRedshiftOutput_1_conntRedshiftOutput_1_1

					.write()
					.format("com.databricks.spark.redshift")
					.option("url",
							"jdbc:redshift://"
									+ "mcdtalendpoc.cj4pclpbavnm.us-west-2.redshift.amazonaws.com"
									+ ":"
									+ "5439"
									+ "/"
									+ "mcdtalendpoc"
									+ "?user="
									+ "slalommcd"
									+ "&password="
									+ routines.system.PasswordEncryptUtil
											.decryptPassword("e30b2b53a2a02395aa4cf535bb233d37")
									+ "")
					.option("dbtable", "public" + "." + "djp_emp_id")
					.option("tempdir", "s3a://" + "dougpressman" + "/tmp")
					.option("diststyle", "EVEN")

					.mode(org.apache.spark.sql.SaveMode.Append).save();
			/**
			 * [tRedshiftOutput_1 sparkconfig ] stop
			 */
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public void tRedshiftConfiguration_1Process(
			final org.apache.spark.api.java.JavaSparkContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
		final JobConf job = new JobConf(ctx.hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public void tS3Configuration_1Process(
			final org.apache.spark.api.java.JavaSparkContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
		final JobConf job = new JobConf(ctx.hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public void tSparkConfiguration_1Process(
			final org.apache.spark.api.java.JavaSparkContext ctx,
			GlobalVar globalMap) throws java.lang.Exception {
		FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
		final JobConf job = new JobConf(ctx.hadoopConfiguration());

		try {
		} catch (java.lang.Exception e) {

			throw e;
		}
	}

	public static class TalendKryoRegistrator implements
			org.apache.spark.serializer.KryoRegistrator {

		@Override
		public void registerClasses(com.esotericsoftware.kryo.Kryo kryo) {
			try {
				kryo.register(Class
						.forName("org.talend.bigdata.dataflow.keys.JoinKeyRecord"));
			} catch (java.lang.ClassNotFoundException e) {
				// Ignore
			}

			kryo.register(java.net.InetAddress.class,
					new InetAddressSerializer());
			kryo.addDefaultSerializer(java.net.InetAddress.class,
					new InetAddressSerializer());

			kryo.register(innovationlab.djp_tst2_0_1.row1Struct.class);

		}
	}

	public static class InetAddressSerializer extends
			com.esotericsoftware.kryo.Serializer<java.net.InetAddress> {

		@Override
		public void write(com.esotericsoftware.kryo.Kryo kryo,
				com.esotericsoftware.kryo.io.Output output,
				java.net.InetAddress value) {
			output.writeInt(value.getAddress().length);
			output.writeBytes(value.getAddress());
		}

		@Override
		public java.net.InetAddress read(com.esotericsoftware.kryo.Kryo kryo,
				com.esotericsoftware.kryo.io.Input input,
				Class<java.net.InetAddress> paramClass) {
			java.net.InetAddress inetAddress = null;
			try {
				int length = input.readInt();
				byte[] address = input.readBytes(length);
				inetAddress = java.net.InetAddress.getByAddress(address);
			} catch (java.net.UnknownHostException e) {
				// Cannot recreate InetAddress instance : return null
			} catch (com.esotericsoftware.kryo.KryoException e) {
				// Should not happen since write() and read() methods are
				// consistent, but if it does happen, it is an unrecoverable
				// error.
				throw new RuntimeException(e);
			}
			return inetAddress;
		}
	}

	public String resuming_logs_dir_path = null;
	public String resuming_checkpoint_path = null;
	public String parent_part_launcher = null;
	public String pid = "0";
	public String rootPid = null;
	public String fatherPid = null;
	public Integer portStats = null;
	public String clientHost;
	public String defaultClientHost = "localhost";
	public String libjars = null;
	private boolean execStat = true;
	public boolean isChildJob = false;
	public String fatherNode = null;
	public String log4jLevel = "";
	private boolean doInspect = false;

	public String contextStr = "Default";
	public boolean isDefaultContext = true;

	private java.util.Properties context_param = new java.util.Properties();
	public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

	public String status = "";

	public static void main(String[] args) throws java.lang.RuntimeException {
		int exitCode = new djp_tst2().runJobInTOS(args);
		if (exitCode == 0) {
			log.info("TalendJob: 'djp_tst2' - Done.");
		} else {
			log.error("TalendJob: 'djp_tst2' - Failed with exit code: "
					+ exitCode + ".");
		}
		if (exitCode == 0) {
			System.exit(exitCode);
		} else {
			throw new java.lang.RuntimeException(
					"TalendJob: 'djp_tst2' - Failed with exit code: "
							+ exitCode + ".");
		}
	}

	public String[][] runJob(String[] args) {
		int exitCode = runJobInTOS(args);
		String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };
		return bufferValue;
	}

	public int runJobInTOS(String[] args) {
		normalizeArgs(args);

		String lastStr = "";
		for (String arg : args) {
			if (arg.equalsIgnoreCase("--context_param")) {
				lastStr = arg;
			} else if (lastStr.equals("")) {
				evalParam(arg);
			} else {
				evalParam(lastStr + " " + arg);
				lastStr = "";
			}
		}

		if (!"".equals(log4jLevel)) {
			if ("trace".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.TRACE);
			} else if ("debug".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.DEBUG);
			} else if ("info".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.INFO);
			} else if ("warn".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.WARN);
			} else if ("error".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.ERROR);
			} else if ("fatal".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.FATAL);
			} else if ("off".equalsIgnoreCase(log4jLevel)) {
				log.setLevel(org.apache.log4j.Level.OFF);
			}
			org.apache.log4j.Logger.getRootLogger().setLevel(log.getLevel());
		}
		log.info("TalendJob: 'djp_tst2' - Start.");

		initContext();

		if (clientHost == null) {
			clientHost = defaultClientHost;
		}

		if (pid == null || "0".equals(pid)) {
			pid = TalendString.getAsciiRandomString(6);
		}

		if (rootPid == null) {
			rootPid = pid;
		}
		if (fatherPid == null) {
			fatherPid = pid;
		} else {
			isChildJob = true;
		}

		String osName = System.getProperty("os.name");
		String snappyLibName = "libsnappyjava.so";
		if (osName.startsWith("Windows")) {
			snappyLibName = "snappyjava.dll";
		} else if (osName.startsWith("Mac")) {
			snappyLibName = "libsnappyjava.jnilib";
		}
		System.setProperty("org.xerial.snappy.lib.name", snappyLibName);
		try {
			java.util.Map<String, String> tuningConf = new java.util.HashMap<>();
			org.apache.spark.SparkConf sparkConfiguration = getConf(tuningConf);
			org.apache.spark.api.java.JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext(
					sparkConfiguration);
			return run(ctx);
		} catch (Exception ex) {
			ex.printStackTrace();
			return 1;
		}
	}

	/**
	 *
	 * This method runs the Spark job using the SparkContext in argument.
	 * 
	 * @param ctx
	 *            , the SparkContext.
	 * @return the Spark job exit code.
	 *
	 */
	private int run(org.apache.spark.api.java.JavaSparkContext ctx)
			throws java.lang.Exception {
		ctx.sc().addSparkListener(new TalendEndOfRunSparkListener(jobName));
		ctx.setJobGroup(projectName + "_" + jobName + "_"
				+ Thread.currentThread().getId(), "");

		initContext();

		setContext(ctx.hadoopConfiguration(), ctx);

		if (doInspect) {
			System.out.println("== inspect start ==");
			System.out.println("{");
			System.out.println("  \"SPARK_MASTER\": \""
					+ ctx.getConf().get("spark.master") + "\",");
			System.out.println("  \"SPARK_UI_PORT\": \""
					+ ctx.getConf().get("spark.ui.port", "4040") + "\",");
			System.out.println("  \"JOB_NAME\": \""
					+ ctx.getConf().get("spark.app.name", jobName) + "\"");
			System.out.println("}"); //$NON-NLS-1$
			System.out.println("== inspect end ==");
		}

		log.info("A <tRedshiftConfiguration_1> configuration component has been found.");
		StringBuilder log4jParamters_tRedshiftConfiguration_1 = new StringBuilder();
		log4jParamters_tRedshiftConfiguration_1.append("Parameters:");
		log4jParamters_tRedshiftConfiguration_1
				.append("HOST"
						+ " = "
						+ "\"mcdtalendpoc.cj4pclpbavnm.us-west-2.redshift.amazonaws.com\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("PORT" + " = "
				+ "\"5439\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("USER" + " = "
				+ "\"slalommcd\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("PASS"
				+ " = "
				+ String.valueOf("e30b2b53a2a02395aa4cf535bb233d37").substring(
						0, 4) + "...");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("DBNAME" + " = "
				+ "\"mcdtalendpoc\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("SCHEMA_DB" + " = "
				+ "\"public\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("PROPERTIES" + " = "
				+ "\"\"");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("S3_CONFIGURATION"
				+ " = " + "tS3Configuration_1");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		log4jParamters_tRedshiftConfiguration_1.append("S3_TEMP_PATH" + " = "
				+ "/tmp");
		log4jParamters_tRedshiftConfiguration_1.append(" | ");
		if (log.isDebugEnabled())
			log.debug("tRedshiftConfiguration_1 - "
					+ log4jParamters_tRedshiftConfiguration_1);
		log.info("A <tS3Configuration_1> configuration component has been found.");
		StringBuilder log4jParamters_tS3Configuration_1 = new StringBuilder();
		log4jParamters_tS3Configuration_1.append("Parameters:");
		log4jParamters_tS3Configuration_1.append("ACCESS_KEY" + " = "
				+ "\"AKIAIAK6HDAVEP52ZYEA\"");
		log4jParamters_tS3Configuration_1.append(" | ");
		log4jParamters_tS3Configuration_1
				.append("SECRET_KEY"
						+ " = "
						+ String.valueOf(
								"4c869c8ce88cb031a30a9a41d9f1de09b06214e4c24d61dd311ef92c21f40fc7f9942e83cbee5e79f4f7aba1746784ea")
								.substring(0, 4) + "...");
		log4jParamters_tS3Configuration_1.append(" | ");
		log4jParamters_tS3Configuration_1.append("BUCKET_NAME" + " = "
				+ "\"dougpressman\"");
		log4jParamters_tS3Configuration_1.append(" | ");
		log4jParamters_tS3Configuration_1.append("TEMP_FOLDER" + " = "
				+ "\"/tmp\"");
		log4jParamters_tS3Configuration_1.append(" | ");
		log4jParamters_tS3Configuration_1.append("USE_S3A" + " = " + "false");
		log4jParamters_tS3Configuration_1.append(" | ");
		log4jParamters_tS3Configuration_1.append("SET_ENDPOINT" + " = "
				+ "false");
		log4jParamters_tS3Configuration_1.append(" | ");
		if (log.isDebugEnabled())
			log.debug("tS3Configuration_1 - "
					+ log4jParamters_tS3Configuration_1);
		log.info("A <tSparkConfiguration_1> configuration component has been found.");
		StringBuilder log4jParamters_tSparkConfiguration_1 = new StringBuilder();
		log4jParamters_tSparkConfiguration_1.append("Parameters:");
		log4jParamters_tSparkConfiguration_1.append("SPARK_LOCAL_MODE" + " = "
				+ "true");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		log4jParamters_tSparkConfiguration_1.append("SPARK_LOCAL_VERSION"
				+ " = " + "SPARK_1_6_0");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		log4jParamters_tSparkConfiguration_1.append("DEFINE_HADOOP_HOME_DIR"
				+ " = " + "false");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		log4jParamters_tSparkConfiguration_1.append("ADVANCED_SETTINGS_CHECK"
				+ " = " + "false");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		log4jParamters_tSparkConfiguration_1.append("SPARK_SCRATCH_DIR" + " = "
				+ "\"/tmp\"");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		log4jParamters_tSparkConfiguration_1.append("SPARK_ADVANCED_PROPERTIES"
				+ " = " + "[]");
		log4jParamters_tSparkConfiguration_1.append(" | ");
		if (log.isDebugEnabled())
			log.debug("tSparkConfiguration_1 - "
					+ log4jParamters_tSparkConfiguration_1);
		try {
			globalMap = new GlobalVar(ctx.hadoopConfiguration());
			tRedshiftInput_1Process(ctx, globalMap);
		} catch (Exception e) {
			e.printStackTrace();
			e.printStackTrace(errorMessagePS);
			throw e;
		} finally {
			ctx.cancelJobGroup(projectName + "_" + jobName + "_"
					+ Thread.currentThread().getId());
			ctx.stop();
		}

		return 0;
	}

	/**
	 *
	 * This method has the responsibility to return a Spark configuration for
	 * the Spark job to run.
	 * 
	 * @return a Spark configuration.
	 *
	 */
	private org.apache.spark.SparkConf getConf(
			java.util.Map<String, String> tuningConf)
			throws java.lang.Exception {
		org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();

		sparkConfiguration.setAppName(projectName + "_" + jobName + "_"
				+ jobVersion);
		sparkConfiguration.set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		sparkConfiguration.set("spark.kryo.registrator",
				TalendKryoRegistrator.class.getName());
		sparkConfiguration.setMaster("local[*]");
		routines.system.GetJarsToRegister getJarsToRegister = new routines.system.GetJarsToRegister();
		java.util.List<String> listJar = new java.util.ArrayList<String>();
		if (libjars != null) {
			String libJarUriPrefix = System.getProperty("os.name").startsWith(
					"Windows") ? "file:///" : "file://";
			for (String jar : libjars.split(",")) {
				listJar.add(getJarsToRegister.replaceJarPaths(jar,
						libJarUriPrefix));
			}
		}
		routines.system.BigDataUtil.installWinutils("/tmp",
				getJarsToRegister.replaceJarPaths("../lib/"
						+ "winutils-hadoop-2.6.0.exe"));
		sparkConfiguration.setJars(listJar.toArray(new String[listJar.size()]));
		tuningConf.put("spark.hadoop.fs.s3.impl",
				"org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		tuningConf.put("spark.hadoop.fs.s3n.impl",
				"org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		tuningConf.put("spark.hadoop.fs.s3.awsAccessKeyId",
				"AKIAIAK6HDAVEP52ZYEA");
		tuningConf.put("spark.hadoop.fs.s3n.awsAccessKeyId",
				"AKIAIAK6HDAVEP52ZYEA");
		tuningConf
				.put("spark.hadoop.fs.s3.awsSecretAccessKey",
						routines.system.PasswordEncryptUtil
								.decryptPassword("4c869c8ce88cb031a30a9a41d9f1de09b06214e4c24d61dd311ef92c21f40fc7f9942e83cbee5e79f4f7aba1746784ea"));
		tuningConf
				.put("spark.hadoop.fs.s3n.awsSecretAccessKey",
						routines.system.PasswordEncryptUtil
								.decryptPassword("4c869c8ce88cb031a30a9a41d9f1de09b06214e4c24d61dd311ef92c21f40fc7f9942e83cbee5e79f4f7aba1746784ea"));
		sparkConfiguration.setAll(scala.collection.JavaConversions
				.asScalaMap(tuningConf));
		sparkConfiguration.set("spark.local.dir", "/tmp");

		return sparkConfiguration;
	}

	private String genTempFolderForComponent(String name) {
		java.io.File tempDir = new java.io.File("/tmp/" + pid, name);
		String tempDirPath = tempDir.getPath();
		if (java.io.File.separatorChar != '/')
			tempDirPath = tempDirPath.replace(java.io.File.separatorChar, '/');
		return tempDirPath;
	}

	private void initContext() {
		// get context
		try {
			// call job/subjob with an existing context, like:
			// --context=production. if without this parameter, there will use
			// the default context instead.
			java.io.InputStream inContext = djp_tst2.class.getClassLoader()
					.getResourceAsStream(
							"innovationlab/djp_tst2_0_1/contexts/" + contextStr
									+ ".properties");
			if (isDefaultContext && inContext == null) {

			} else {
				if (inContext != null) {
					// defaultProps is in order to keep the original context
					// value
					defaultProps.load(inContext);
					inContext.close();
					context = new ContextProperties(defaultProps);
				} else {
					// print info and job continue to run, for case:
					// context_param is not empty.
					System.err.println("Could not find the context "
							+ contextStr);
				}
			}

			if (!context_param.isEmpty()) {
				context.putAll(context_param);
			}
			context.loadValue(context_param, null);
			if (parentContextMap != null && !parentContextMap.isEmpty()) {
			}
		} catch (java.io.IOException ie) {
			System.err.println("Could not load context " + contextStr);
			ie.printStackTrace();
		}
	}

	private void setContext(Configuration conf,
			org.apache.spark.api.java.JavaSparkContext ctx) {
		// get context
		// call job/subjob with an existing context, like: --context=production.
		// if without this parameter, there will use the default context
		// instead.
		java.net.URL inContextUrl = djp_tst2.class.getClassLoader()
				.getResource(
						"innovationlab/djp_tst2_0_1/contexts/" + contextStr
								+ ".properties");
		if (isDefaultContext && inContextUrl == null) {

		} else {
			if (inContextUrl != null) {
				conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr
						+ ".properties");

			}
		}

		if (!context_param.isEmpty()) {
			for (Object contextKey : context_param.keySet()) {
				conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX + contextKey,
						context.getProperty(contextKey.toString()));
				conf.set(ContextProperties.CONTEXT_KEYS,
						conf.get(ContextProperties.CONTEXT_KEYS, "") + " "
								+ contextKey);
			}
		}

		if (parentContextMap != null && !parentContextMap.isEmpty()) {
		}
	}

	private void evalParam(String arg) {
		if (arg.startsWith("--resuming_logs_dir_path")) {
			resuming_logs_dir_path = arg.substring(25);
		} else if (arg.startsWith("--resuming_checkpoint_path")) {
			resuming_checkpoint_path = arg.substring(27);
		} else if (arg.startsWith("--parent_part_launcher")) {
			parent_part_launcher = arg.substring(23);
		} else if (arg.startsWith("--father_pid=")) {
			fatherPid = arg.substring(13);
		} else if (arg.startsWith("--root_pid=")) {
			rootPid = arg.substring(11);
		} else if (arg.startsWith("--pid=")) {
			pid = arg.substring(6);
		} else if (arg.startsWith("--context=")) {
			contextStr = arg.substring("--context=".length());
			isDefaultContext = false;
		} else if (arg.startsWith("--context_param")) {
			String keyValue = arg.substring("--context_param".length() + 1);
			int index = -1;
			if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
				context_param.put(keyValue.substring(0, index),
						replaceEscapeChars(keyValue.substring(index + 1)));
			}
		} else if (arg.startsWith("--stat_port=")) {
			String portStatsStr = arg.substring(12);
			if (portStatsStr != null && !portStatsStr.equals("null")) {
				portStats = Integer.parseInt(portStatsStr);
			}
		} else if (arg.startsWith("--client_host=")) {
			clientHost = arg.substring(14);
		} else if (arg.startsWith("--log4jLevel=")) {
			log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--inspect")) {
			doInspect = Boolean.valueOf(arg.substring("--inspect=".length()));
		}
	}

	private void normalizeArgs(String[] args) {
		java.util.List<String> argsList = java.util.Arrays.asList(args);
		int indexlibjars = argsList.indexOf("-libjars") + 1;
		libjars = indexlibjars == 0 ? null : argsList.get(indexlibjars);
	}

	private final String[][] escapeChars = { { "\\\\", "\\" }, { "\\n", "\n" },
			{ "\\'", "\'" }, { "\\r", "\r" }, { "\\f", "\f" }, { "\\b", "\b" },
			{ "\\t", "\t" } };

	private String replaceEscapeChars(String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0], currIndex);
				if (index >= 0) {

					result.append(keyValue.substring(currIndex,
							index + strArray[0].length()).replace(strArray[0],
							strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left
			// into the result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
	}

	public String getStatus() {
		return status;
	}
}
