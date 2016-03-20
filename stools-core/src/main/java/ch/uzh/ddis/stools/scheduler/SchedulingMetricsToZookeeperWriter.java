/**
 *  @author Lorenz Fischer
 *
 *  Copyright 2016 University of Zurich
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package ch.uzh.ddis.stools.scheduler;


import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class consumes the metrics that are collected by the {@link ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook}
 * and writes them into zookeeper.
 * <p/>
 * You can configure it in the storm.yaml file as:
 * <pre>
 *   topology.metrics.consumer.register:
 *     - class: "ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter"
 *       parallelism.hint: 1
 * </pre>
 * <p/>
 * The metrics (the send graph information) is written into a node with the name of the topology in Zookeeper
 * ({@see #getConfiguredMetricsZookeeperRootPath}). In order to minimize the number of requests to Zookeeper, we first
 * collect the data into an internal memory structure and only write to zookeeper if we have not received any metrics
 * for {@link SchedulingMetricsToZookeeperWriter#DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS}
 * ({@see SchedulingMetricsToZookeeperWriter#CONF_SCHEDULING_METRIC_TIMEOUT_SECS}) seconds <b>or</b> if there
 * has not been an update for
 * {@link ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook#getConfiguredSchedulingIntervalSecs(java.util.Map)}
 * seconds.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SchedulingMetricsToZookeeperWriter implements IMetricsConsumer {

    /**
     * The collected metrics will be written into subfolders under this path. The subfolders
     * are named after the topology names.
     * <p/>
     * Example: /stools/scheduler/metrics/topoXYZ
     * <p/>
     * Under the scheduler paths there are two folders, one containing the sendgraph data and one containing general
     * configuration information about the topology. The two folder look accordingly:
     * <ul>
     * <li>/stools/scheduler/metrics/topoXYZ/sendgraph</li>
     * <li>/stools/scheduler/metrics/topoXYZ/configuration</li>
     * </ul>
     */
    public static final String DEFAULT_SCHEDULING_METRICS_ZK_PATH = "/stools/scheduler/metrics";

    /**
     * If a property with this name can be found in the storm configuration object the root path
     * in zookeeper will be set to the value of the provided property.
     */
    public static final String CONF_SCHEDULING_METRICS_ZK_PATH = "scheduling.metrics.zk.path";

    /**
     * The default time out during which a metric update is still being considered as part of the "current
     * update cycle". This can be overridden by configuring
     * {@see SchedulingMetricsToZookeeperWriter#CONF_SCHEDULING_METRIC_TIMEOUT_SECS}.
     */
    public static final int DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS = 5;

    /**
     * If a name with this property is configured in the storm configuration map, its value will
     * be used as the timeout during which a metric update is still being considered as part of the "current
     * update cycle.
     */
    public static final String CONF_SCHEDULING_METRIC_TIMEOUT_SECS = "scheduling.metrics.timeout.secs";

    private final static Logger LOG = LoggerFactory.getLogger(SchedulingMetricsToZookeeperWriter.class);


    /**
     * This object is used for all communication with Zookeeper.
     */
    private static CuratorFramework zkClient;

    /**
     * The collected sendgrpah will be stored in this path.
     */
    private String zookeeperSendgraphPath;

    /**
     * The timeout during which consecutive updates of metrics are considered to be during the same
     * "update cycle".
     */
    private int updateTimeoutSecs;

    /**
     * We use this variable to compute the time between two updates.
     */
    private AtomicLong lastZookeeperUpdate = new AtomicLong();

    /**
     * This executor is used to schedule the writer task.
     */
    private ScheduledThreadPoolExecutor writerTaskExecutor;

    /**
     * This task will be executed after each update cycle. It will write the contents of the sendgraph into
     * zookeeper unless the task is cancelled before it can do do.
     */
    private Runnable dateUpdateTask;

    /**
     * This reference is used to cancel an already scheduled writer task, should more data have been received since we
     * started the timer the last time.
     */
    private Future<?> taskFuture;

    /**
     * This object contains all the data we receive through the metrics framework. The values in it are not summed
     * up, but overridden.
     */
    private Sendgraph sendGraph;

    /**
     * We store the interval in which the taks hooks send data to the metrics framework, so we can issue an update
     * of zookeeper at the latest, when this interval has expired.
     */
    private int metricsCollectionIntervalSecsMills;

    /**
     * This method computes and returns the path in Zookeeper under which the send graphs are stored. Because
     * this path is configurable through the property {@value #CONF_SCHEDULING_METRICS_ZK_PATH} the storm
     * configuration map must be passed as an argument.
     * <p/>
     * For each topology, there will be a sub node in this path, which in turn contains the send graph data. For example
     * the number of submitted messages from task 2 to task 6 of topology test-topo-1-1394729310 would be stored in:<br/>
     * <pre>
     * {rootPath}/test-topo-1-1394729310/2-6
     * </pre>
     *
     * @param stormConf the storm configuration map.
     * @return the path in Zookeeper under which the sendgraph data is stored.
     */
    public static String getConfiguredMetricsZookeeperRootPath(Map stormConf) {
        String path;

        if (stormConf.containsKey(CONF_SCHEDULING_METRICS_ZK_PATH)) {
            path = (String) stormConf.get(CONF_SCHEDULING_METRICS_ZK_PATH);
        } else {
            path = DEFAULT_SCHEDULING_METRICS_ZK_PATH;
        }

        return path;
    }


    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        final IErrorReporter errorReporter) {
        String zkTopologyRootPath;
        String zkTopologyConfigPath;

        LOG.info("Initializing the " + getClass().getCanonicalName());

        // open a connection to zookeeper
        this.zkClient = Stootils.getConfiguredZkClient(stormConf);

        zkTopologyRootPath = getConfiguredMetricsZookeeperRootPath(stormConf) + "/" + stormConf.get(Config.TOPOLOGY_NAME);
        zkTopologyConfigPath = zkTopologyRootPath + "/config";
        this.zookeeperSendgraphPath = zkTopologyRootPath + "/sendgraph";

        // store configuration options in zookeeper
        try {
            String numWorkerZkPath;
            Object numWorkerZkObject;
            byte[] numWorkerData;

            numWorkerZkPath = zkTopologyConfigPath + "/" + Config.TOPOLOGY_WORKERS;
            numWorkerZkObject = stormConf.get(Config.TOPOLOGY_WORKERS);
            numWorkerData = ByteBuffer.allocate(8).putLong(((Number)numWorkerZkObject).longValue()).array();
            if (zkClient.checkExists().forPath(numWorkerZkPath) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(numWorkerZkPath, numWorkerData);
            } else {
                zkClient.setData().forPath(numWorkerZkPath, numWorkerData);
            }

        } catch (Exception e) {
            String errorMsg = "Couldn't write configuration values into ZK for " + zkTopologyRootPath;
            LOG.error(errorMsg);
            errorReporter.reportError(new RuntimeException(errorMsg, e));
        }

        // create the root path in zookeeper
        try {
            if (zkClient.checkExists().forPath(this.zookeeperSendgraphPath) == null) { // null = doesn't exist
                zkClient.create().creatingParentsIfNeeded().forPath(this.zookeeperSendgraphPath);
            }
        } catch (Exception e) {
            String errorMsg = "Couldn't create root folder in ZK for " + zkTopologyRootPath;
            LOG.error(errorMsg);
            errorReporter.reportError(new RuntimeException(errorMsg, e));
        }

        this.updateTimeoutSecs = DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS;
        if (stormConf.containsKey(CONF_SCHEDULING_METRIC_TIMEOUT_SECS)) {
            this.updateTimeoutSecs = Utils.getInt(stormConf.get(CONF_SCHEDULING_METRIC_TIMEOUT_SECS));
        }

        // create the sendgraph object
        this.sendGraph = new Sendgraph();

        this.metricsCollectionIntervalSecsMills =
                SchedulingMetricsCollectionHook.getConfiguredSchedulingIntervalSecs(stormConf) * 1000;

        this.writerTaskExecutor = new ScheduledThreadPoolExecutor(1);
        this.dateUpdateTask = new Runnable() {
            @Override
            public void run() {
                LOG.trace("Updating sendgraph in ZK for path {}", zookeeperSendgraphPath);
                try {
                    String sendgraphInJson;

                    sendgraphInJson = sendGraph.toJson();
                    zkClient.setData().forPath(zookeeperSendgraphPath, sendgraphInJson.getBytes());
                    lastZookeeperUpdate.set(System.currentTimeMillis());
                } catch (Exception e) {
                    String errorMsg = "Couldn't write json sendgraph into Zookeeper";
                    LOG.error(errorMsg);
                }


            }
        };
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        boolean atLeastOneValueUpdated = false;
        int taskId;

        taskId = taskInfo.srcTaskId;

        for (DataPoint dp : dataPoints) {
            //LOG.trace("Datapoints for task {} and metric {}", taskId, dp.name);
            if (SchedulingMetricsCollectionHook.METRIC_EMITTED_MESSAGES.equals(dp.name)) {
                LOG.trace("Datapoint of task {}: {}", taskId, dp.value.toString());

                for (Map.Entry<Integer, AtomicLong> toTaskCount : ((Map<Integer, AtomicLong>) dp.value).entrySet()) {
                    // write the data our internal data structure
                    this.sendGraph.setEdgeWeight(Integer.valueOf(taskId), // fromTaskId
                            toTaskCount.getKey(),     // toTaskId
                            Long.valueOf(toTaskCount.getValue().get())); // messageCount
                    atLeastOneValueUpdated = true;
                }
            }
        }

        if (atLeastOneValueUpdated) {
            // Reset the current timeout counter and re-schedule the date updater task if the last update
            // was made less than metricsCollectionIntervalSecsMills ago. If not, we don't reset the counter to
            // prevent the situation in which we never write anything to zookeeper.
            if (this.lastZookeeperUpdate.longValue() == 0 || // make sure the first scheduling happens
                    (this.lastZookeeperUpdate.longValue() + metricsCollectionIntervalSecsMills)
                            > System.currentTimeMillis()) {
                // ok last update was not too long ago, so we can cancel (and re-schedule) the task
                LOG.trace("Resetting timer for zookeeper writer, lastupdate={}, currentTimeMillis={}",
                        this.lastZookeeperUpdate.longValue(), System.currentTimeMillis());
                if (taskFuture == null || taskFuture.cancel(false) || taskFuture.isDone()) {
                    taskFuture = writerTaskExecutor.schedule(dateUpdateTask, updateTimeoutSecs,
                            TimeUnit.SECONDS);
                } else {
                    LOG.warn("Could not (re-) schedule the date update task.");
                }
            }
        }
    }

    @Override
    public void cleanup() {
        this.zkClient.close();
    }
}
