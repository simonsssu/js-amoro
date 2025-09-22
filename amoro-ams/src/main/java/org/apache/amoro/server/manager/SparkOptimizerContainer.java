/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.amoro.server.manager;

import com.ebay.hadoop.kite2.client.config.Configs;
import org.apache.amoro.OptimizerProperties;
import org.apache.amoro.resource.Resource;
import org.apache.amoro.server.Environments;
import org.apache.amoro.server.manager.kyuubi.HadoopUtils;
import org.apache.amoro.server.manager.kyuubi.KyuubiUtil;
import org.apache.amoro.server.utils.SparkConfUtil;
import org.apache.amoro.shade.guava32.com.google.common.base.Function;
import org.apache.amoro.shade.guava32.com.google.common.base.Preconditions;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kyuubi.client.BatchRestApi;
import org.apache.kyuubi.client.KyuubiRestClient;
import org.apache.kyuubi.client.api.v1.dto.Batch;
import org.apache.kyuubi.client.api.v1.dto.BatchRequest;
import org.apache.kyuubi.client.api.v1.dto.OperationLog;
import org.apache.kyuubi.client.util.BatchUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SparkOptimizerContainer extends AbstractOptimizerContainer {
  private static final Logger LOG = LoggerFactory.getLogger(SparkOptimizerContainer.class);

  public static final String SPARK_HOME_PROPERTY = "spark-home";
  public static final String ENV_HADOOP_USER_NAME = "HADOOP_USER_NAME";
  private static final String DEFAULT_JOB_URI = "/plugin/optimizer/spark/optimizer-job.jar";
  private static final String SPARK_JOB_MAIN_CLASS =
      "org.apache.amoro.optimizer.spark.SparkOptimizer";

  public static final String SUBMIT_MODE = "submit-mode";

  public static final String SPARK_MASTER = "master";
  public static final String SPARK_DEPLOY_MODE = "deploy-mode";
  public static final String SPARK_JOB_URI = "job-uri";
  public static final String YARN_APPLICATION_ID_PROPERTY = "yarn-application-id";
  public static final String KUBERNETES_SUBMISSION_ID_PROPERTY = "kubernetes-submission-id";
  private static final Pattern APPLICATION_ID_PATTERN =
      Pattern.compile("(.*)application_(\\d+)_(\\d+)");
  private static final int MAX_READ_APP_ID_TIME = 600000; // 10 min
  private static final Function<String, String> yarnApplicationIdReader =
      readLine -> {
        Matcher matcher = APPLICATION_ID_PATTERN.matcher(readLine);
        if (matcher.matches()) {
          return String.format("application_%s_%s", matcher.group(2), matcher.group(3));
        }
        return null;
      };
  private String sparkMaster;
  private DeployMode deployMode;
  private String sparkHome;
  private String jobUri;

  private BatchRequest batchRequest;
  private SubmitMode submitMode;

  @Override
  public void init(String name, Map<String, String> containerProperties) {
    super.init(name, containerProperties);
    this.submitMode =
        SubmitMode.valueToEnum(
            containerProperties.getOrDefault(SUBMIT_MODE, SubmitMode.COMMON.getValue()));
    if (submitMode == SubmitMode.KYUUBI) {
      LOG.info("Using kyuubi submit mode to start spark optimizer.");
      this.sparkHome = getSparkHome();
      this.batchRequest = new BatchRequest();
      batchRequest.setBatchType("spark");
      batchRequest.setResource("spark-internal");
      batchRequest.setClassName(SPARK_JOB_MAIN_CLASS);
      String jobUri = containerProperties.get(SPARK_JOB_URI);
      if (StringUtils.isEmpty(jobUri)) {
        throw new IllegalArgumentException(
            "The property: job-uri is required when submit-mode is kyuubi.");
      }
      batchRequest.setResource(jobUri);

      Map<String, String> sparkConf =
          new HashMap<>() {
            {
              put("kyuubi.session.tag", "amoro");
              put("kyuubi.session.cluster", "apollorno");

              put("spark.sql.bucketing.coalesceBucketsInJoin.enabled", "true");
              put("spark.driver.maxResultSize", "6g");
              put("spark.executor.num", "100");
              put("spark.sql.shuffle.partitions", "3000");
              put("spark.sql.broadcastTimeout", "3000");
              put("spark.sql.sources.bucketing.enabled", "true");
              put("spark.default.parallelism", "3000");
              put("spark.sql.adaptive.enabled", "true");
              put("spark.sql.adaptive.coalescePartitions.enabled", "true");

              put("spark.kryoserializer.buffer", "2m");
              put("spark.dynamicAllocation.minExecutors", "100");
              put("spark.executor.memoryOverhead", "4g");
              put("spark.sql.sources.v2.bucketing.enabled", "true");
              put("spark.yarn.preserve.staging.files", "true");
              //
              //              put(
              //                  "spark.kerberos.access.hadoopFileSystems",
              //
              // "viewfs://apollo-rno,viewfs://hermes-rno,hdfs://hermes-rno,hdfs://hermes-rno-ns01");

              put("spark.sql.adaptive.skewJoin.enabled", "true");
              put("spark.executor.memory", "40g");
              put("spark.driver.memory", "20g");
              put("spark.kryoserializer.buffer.max", "1024m");
              put("spark.driver.cores", "4");
              put("spark.executor.heartbeatInterval", "20s");
              put("spark.yarn.maxAppAttempts", "0");
              put("spark.executor.cores", "1");
              put("spark.binary.majorVersion", "3.5.0");

              put("spark.sql.files.maxPartitionBytes", "1g");
              put("spark.dynamicAllocation.maxExecutors", "400");
              put("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true");
              put("spark.sql.sources.partitionOverwriteMode", "dynamic");
              put("spark.yarn.max.executor.failures", "100");
              put("spark.dynamicAllocation.enabled", "true");

              put("spark.yarn.queue", "hdlq-gdi-default");

              put("spark.app.name", "amoro-compaction-job");
              put("spark.kyuubi.batch.etl.sql.encoded.statements", "");
              //              put("hive.server2.proxy.user", "b_rheos");
            }
          };
      batchRequest.setConf(sparkConf);

    } else {
      this.sparkHome = getSparkHome();
      this.sparkMaster = containerProperties.getOrDefault(SPARK_MASTER, "yarn");
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(sparkMaster), "The property: %s is required", sparkMaster);
      String runMode =
          Optional.ofNullable(containerProperties.get(SPARK_DEPLOY_MODE))
              .orElse(DeployMode.CLIENT.getValue());
      this.deployMode = DeployMode.valueToEnum(runMode);
      String jobUri = containerProperties.get(SPARK_JOB_URI);
      if (deployMode.equals(DeployMode.CLUSTER.name())) {
        Preconditions.checkArgument(
            StringUtils.isNotEmpty(jobUri),
            "The property: %s is required if running mode in cluster mode.",
            SPARK_JOB_URI);
      }
      if (StringUtils.isEmpty(jobUri)) {
        jobUri = amsHome + DEFAULT_JOB_URI;
      }
      this.jobUri = jobUri;
      SparkConfUtil sparkConf =
          SparkConfUtil.buildFor(loadSparkConfig(), containerProperties).build();
      if (deployedOnKubernetes()) {
        String imageRef =
            sparkConf.configValue(SparkOptimizerContainer.SparkConfKeys.KUBERNETES_IMAGE_REF);
        Preconditions.checkArgument(
            StringUtils.isNotEmpty(imageRef),
            "The spark-conf: %s is required if running mode is %s",
            SparkOptimizerContainer.SparkConfKeys.KUBERNETES_IMAGE_REF,
            deployMode.getValue());
      }
    }
  }

  @Override
  protected Map<String, String> doScaleOut(Resource resource) {
    try {
      if (submitMode == SubmitMode.KYUUBI) {
        LOG.info("Submit via kyuubi.");
        List<String> jobArgs =
            Arrays.asList(super.buildOptimizerStartupArgsString(resource).trim().split("\\s+"));
        LOG.info("Starting spark optimizer using kyuubi with args: {}", jobArgs);
        batchRequest.setArgs(jobArgs);
        Map<String, String> startupStatesMap = Maps.newHashMap();
        startupStatesMap.put("kyuubi-batch-id", submitViaKyuubi(batchRequest));
        return startupStatesMap;
      } else {
        String startUpArgs = this.buildOptimizerStartupArgsString(resource);
        String exportCmd = String.join(" && ", exportSystemProperties());
        String startUpCmd = String.format("%s && %s", exportCmd, startUpArgs);
        String[] cmd = {"/bin/sh", "-c", startUpCmd};
        LOG.info("Starting spark optimizer using command : {}", startUpCmd);
        Process exec = Runtime.getRuntime().exec(cmd);
        Map<String, String> startUpStatesMap = Maps.newHashMap();
        if (deployedOnKubernetes()) {
          SparkConfUtil sparkConf =
              SparkConfUtil.buildFor(loadSparkConfig(), getContainerProperties())
                  .withGroupProperties(resource.getProperties())
                  .build();
          String namespace =
              StringUtils.defaultIfEmpty(
                  sparkConf.configValue(SparkConfKeys.KUBERNETES_NAMESPACE), "default");
          startUpStatesMap.put(
              KUBERNETES_SUBMISSION_ID_PROPERTY,
              String.format("%s:%s", namespace, kubernetesDriverName(resource)));
        } else {
          String applicationId = fetchCommandOutput(exec, yarnApplicationIdReader);
          if (applicationId != null) {
            startUpStatesMap.put(YARN_APPLICATION_ID_PROPERTY, applicationId);
          }
        }
        return startUpStatesMap;
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to scale out spark optimizer.", e);
    }
  }

  @Override
  protected String buildOptimizerStartupArgsString(Resource resource) {
    Map<String, String> sparkConfig = loadSparkConfig();
    SparkConfUtil resourceSparkConf =
        SparkConfUtil.buildFor(sparkConfig, getContainerProperties())
            .withGroupProperties(resource.getProperties())
            .build();

    // Default enable the spark DRA(dynamic resource allocation)
    resourceSparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_DRA_ENABLED,
        StringUtils.defaultIfEmpty(
            resourceSparkConf.configValue(SparkConfKeys.KUBERNETES_DRA_ENABLED), "true"));
    resourceSparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_DRA_MAX_EXECUTORS,
        StringUtils.defaultIfEmpty(
            resourceSparkConf.configValue(SparkConfKeys.KUBERNETES_DRA_MAX_EXECUTORS),
            String.valueOf(resource.getThreadCount())));

    if (deployedOnKubernetes()) {
      addKubernetesProperties(resource, resourceSparkConf);
    }
    String sparkOptions = resourceSparkConf.toConfOptions();
    String proxyUser =
        getContainerProperties()
            .getOrDefault(
                OptimizerProperties.EXPORT_PROPERTY_PREFIX + ENV_HADOOP_USER_NAME, "hadoop");
    String jobArgs = super.buildOptimizerStartupArgsString(resource);
    // ./bin/spark-submit --master <master> --deploy-mode=<sparkMode> <options> --proxy-user <user>
    // --class
    // <main-class>
    // <job-file>
    // <arguments>
    //  options: --conf <property=value>
    return String.format(
        "%s/bin/spark-submit --master %s --deploy-mode=%s %s --proxy-user %s --class %s %s %s",
        sparkHome,
        sparkMaster,
        deployMode.getValue(),
        sparkOptions,
        proxyUser,
        SPARK_JOB_MAIN_CLASS,
        jobUri,
        jobArgs);
  }

  private Map<String, String> loadSparkConfig() {
    try {
      return Arrays.stream(new org.apache.spark.SparkConf().getAll())
          .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
    } catch (Exception e) {
      LOG.error("Load spark conf failed.", e);
      return Collections.emptyMap();
    }
  }

  private boolean deployedOnKubernetes() {
    return sparkMaster.startsWith("k8s://");
  }

  private void addKubernetesProperties(Resource resource, SparkConfUtil sparkConf) {
    String driverName = kubernetesDriverName(resource);
    sparkConf.putToOptions(
        SparkOptimizerContainer.SparkConfKeys.KUBERNETES_DRIVER_NAME, driverName);

    // add labels to the driver pod
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_DRIVER_LABEL_PREFIX + "optimizer-group", resource.getGroupName());
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_DRIVER_LABEL_PREFIX + "optimizer-implementation",
        "spark-native-kubernetes");
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_DRIVER_LABEL_PREFIX + "optimizer-id", resource.getResourceId());

    // add labels to the executor pod
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_EXECUTOR_LABEL_PREFIX + "optimizer-group",
        resource.getGroupName());
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_EXECUTOR_LABEL_PREFIX + "optimizer-implementation",
        "spark-native-kubernetes");
    sparkConf.putToOptions(
        SparkConfKeys.KUBERNETES_EXECUTOR_LABEL_PREFIX + "optimizer-id", resource.getResourceId());
  }

  private <T> T fetchCommandOutput(Process exec, Function<String, T> commandReader) {
    T value = null;
    try (InputStreamReader inputStreamReader = new InputStreamReader(exec.getInputStream())) {
      try (BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < MAX_READ_APP_ID_TIME) {
          String readLine = bufferedReader.readLine();
          if (readLine == null) {
            break;
          }
          LOG.info("{}", readLine);
          if ((value = commandReader.apply(readLine)) != null) {
            break;
          }
        }
        return value;
      }
    } catch (IOException e) {
      LOG.error("Read application id from output failed", e);
      return null;
    }
  }

  @Override
  public void releaseResource(Resource resource) {
    String releaseCommand;
    LOG.info("Spark master is {}", sparkMaster);
    if (StringUtils.isEmpty(sparkMaster)) {
      LOG.info("Use kyuubi to release.");
      String kyuubiId = resource.getProperties().get("kyuubi-batch-id");
      releaseViaKyuubi(kyuubiId);
    } else {
      if (deployedOnKubernetes()) {
        releaseCommand = buildReleaseKubernetesCommand(resource);
      } else {
        releaseCommand = buildReleaseYarnCommand(resource);
      }
      try {
        String exportCmd = String.join(" && ", exportSystemProperties());
        String releaseCmd = exportCmd + " && " + releaseCommand;
        String[] cmd = {"/bin/sh", "-c", releaseCmd};
        LOG.info("Releasing spark optimizer using command: {}", releaseCmd);
        Runtime.getRuntime().exec(cmd);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to release spark optimizer.", e);
      }
    }
  }

  private String buildReleaseYarnCommand(Resource resource) {
    Preconditions.checkArgument(
        resource.getProperties().containsKey(YARN_APPLICATION_ID_PROPERTY),
        "Cannot find {} from optimizer start up stats",
        YARN_APPLICATION_ID_PROPERTY);
    String applicationId = resource.getProperties().get(YARN_APPLICATION_ID_PROPERTY);
    return String.format(
        "%s/bin/spark-submit --kill %s --master %s", sparkHome, applicationId, sparkMaster);
  }

  private String buildReleaseKubernetesCommand(Resource resource) {
    Map<String, String> sparkConfig = loadSparkConfig();
    Preconditions.checkArgument(
        resource.getProperties().containsKey(KUBERNETES_SUBMISSION_ID_PROPERTY),
        "Cannot find {} from optimizer start up stats.",
        KUBERNETES_SUBMISSION_ID_PROPERTY);
    SparkConfUtil resourceSparkConf =
        SparkConfUtil.buildFor(sparkConfig, getContainerProperties())
            .withGroupProperties(resource.getProperties())
            .build();
    String sparkOptions = resourceSparkConf.toConfOptions();
    String submissionId = resource.getProperties().get(KUBERNETES_SUBMISSION_ID_PROPERTY);
    return String.format(
        "%s/bin/spark-submit --kill %s --master %s %s",
        sparkHome, submissionId, sparkMaster, sparkOptions);
  }

  private String getSparkHome() {
    String sparkHome = getContainerProperties().get(SPARK_HOME_PROPERTY);
    Preconditions.checkNotNull(
        sparkHome, "Container property: %s is required", SPARK_HOME_PROPERTY);
    return sparkHome.replaceAll("/$", "");
  }

  private String kubernetesDriverName(Resource resource) {
    return "amoro-optimizer-" + resource.getResourceId();
  }

  private void releaseViaKyuubi(String batchId) {
    try (KyuubiRestClient client = KyuubiUtil.getKyuubiClient()) {
      BatchRestApi batchRestApi = new BatchRestApi(client);
      batchRestApi.deleteBatch(batchId);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create KyuubiRestClient", e);
    }
  }

  private String submitViaKyuubi(BatchRequest batchRequest) {
    //////////////////////////////////////////////////////////////////
    String batchId;
    try (KyuubiRestClient client = KyuubiUtil.getKyuubiClient()) {
      BatchRestApi batchRestApi = new BatchRestApi(client);
      Properties kite2Properties = new Properties();
      String kite2ConfPath = Environments.getConfigPath() + "/" + "kite2.properties";
      LOG.info("Loading kite2 properties from {}", kite2ConfPath);
      kite2Properties.load(Files.newInputStream(Paths.get(kite2ConfPath)));
      String ticketCache = Configs.CONF_TICKET_CACHE_LOCATION.load(kite2Properties);
      String principal = Configs.CONF_USER_PRINCIPAL.load(kite2Properties);

      LOG.info("Using ticket cache: {}, principal: {}", ticketCache, principal);

      batchId =
          HadoopUtils.doAs(
              ticketCache,
              principal,
              () -> {
                return batchRestApi.createBatch(batchRequest).getId();
              });

      /** Set it to true if you want to wait it finished. */
      boolean waitAppCompletion = false;
      int logLineOffset = 0;
      while (true) {
        UserGroupInformation ugi =
            UserGroupInformation.getUGIFromTicketCache(ticketCache, principal);
        Thread.sleep(3000);

        // fetch 100 lines logs per time
        final int finalLogLineOffset = logLineOffset; // used in lambda expression only
        OperationLog log =
            HadoopUtils.doAs(
                ugi,
                () -> {
                  return batchRestApi.getBatchLocalLog(batchId, finalLogLineOffset, 100);
                });

        log.getLogRowSet().forEach(line -> LOG.info("Kuyybi --->: " + line));
        logLineOffset += log.getRowCount();

        Batch latestBatchReport = null;

        if (log.getRowCount() == 0) {
          // if no log fetched, check the batch state
          latestBatchReport =
              HadoopUtils.doAs(
                  ugi,
                  () -> {
                    return batchRestApi.getBatchById(batchId);
                  });
          if (BatchUtils.isTerminalState(latestBatchReport.getState())) {
            LOG.info("The batch has been terminated: " + latestBatchReport);
            break;
          }
        }

        if (latestBatchReport == null) {
          latestBatchReport =
              HadoopUtils.doAs(
                  ugi,
                  () -> {
                    return batchRestApi.getBatchById(batchId);
                  });
          if (!waitAppCompletion && !StringUtils.isBlank(latestBatchReport.getAppId())) {
            LOG.info("The batch has been submitted: " + latestBatchReport);
            break;
          }
        }
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed to create KyuubiRestClient", e);
    }
    return batchId;
  }

  private enum SubmitMode {
    KYUUBI("kyuubi"),
    COMMON("common");
    private final String value;

    SubmitMode(String value) {
      this.value = value;
    }

    public static SubmitMode valueToEnum(String value) {
      return Arrays.stream(values())
          .filter(t -> t.value.equalsIgnoreCase(value))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("can't parse value: " + value));
    }

    public String getValue() {
      return value;
    }
  }

  private enum DeployMode {
    CLIENT("client"),
    CLUSTER("cluster");

    private final String value;

    DeployMode(String value) {
      this.value = value;
    }

    public static DeployMode valueToEnum(String value) {
      return Arrays.stream(values())
          .filter(t -> t.value.equalsIgnoreCase(value))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("can't parse value: " + value));
    }

    public String getValue() {
      return value;
    }
  }

  public static class SparkConfKeys {
    public static final String KUBERNETES_IMAGE_REF = "spark.kubernetes.container.image";
    public static final String KUBERNETES_DRIVER_NAME = "spark.kubernetes.driver.pod.name";
    public static final String KUBERNETES_NAMESPACE = "spark.kubernetes.namespace";
    public static final String KUBERNETES_EXECUTOR_LABEL_PREFIX = "spark.kubernetes.driver.label.";
    public static final String KUBERNETES_DRIVER_LABEL_PREFIX = "spark.kubernetes.driver.label.";
    public static final String KUBERNETES_DRA_ENABLED = "spark.dynamicAllocation.enabled";
    public static final String KUBERNETES_DRA_MAX_EXECUTORS =
        "spark.dynamicAllocation.maxExecutors";
  }
}
