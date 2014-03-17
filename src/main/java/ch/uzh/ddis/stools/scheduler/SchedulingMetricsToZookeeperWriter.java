/* TODO: License */
package ch.uzh.ddis.stools.scheduler;


import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.api.BackgroundCallback;
import com.netflix.curator.framework.api.CuratorEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
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
 *     - class: "backtype.storm.metrics.LoggingMetricsConsumer"
 *       parallelism.hint: 1
 * </pre>
 * <p/>
 * The metrics (the send graph information) is written into a nodes that with the name of the topology in Zookeeper
 * ({@see #getConfiguredMetricsZookeeperRootPath}). In order for a Zookeeper-watcher to know when all data is ready for
 * partitioning, we set the date of the last update as a long value on the node of the given topology as when no new
 * metric data point was received during a certain time. The value of this time is
 * {@value SchedulingMetricsToZookeeperWriter#DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS}
 * seconds, but can be overriden by setting a value for
 * {@link SchedulingMetricsToZookeeperWriter#CONF_SCHEDULING_METRIC_TIMEOUT_SECS}
 * in the storm configuration map.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SchedulingMetricsToZookeeperWriter implements IMetricsConsumer {

    /**
     * The collected metrics will be written into subfolders under this path. The subfolders
     * are named after the topology names.
     * <p/>
     * Example: /stools/scheduler/metrics/topoXYZ
     */
    public static final String DEFAULT_SCHEDULING_METRICS_ZK_PATH = "/stools/scheduler/sendgraph";

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
    public static final int DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS = 10;

    /**
     * If a name with this property is configured in the storm configuration map, its value will
     * be used as the timeout during which a metric update is still being considered as part of the "current
     * update cycle.
     */
    public static final String CONF_SCHEDULING_METRIC_TIMEOUT_SECS = "scheduling.metrics.timeout.secs";

    private final static Logger LOG = LoggerFactory.getLogger(SchedulingMetricsToZookeeperWriter.class);

    /**
     * The collected metrics will be stored in this path.
     */
    private String zookeeperRootPath;

    /**
     * The timeout during which consecutive updates of metrics are considered to be during the same
     * "update cycle".
     */
    private int updateTimeoutSecs;

    /**
     * We use this variable to compute the time between two updates.
     */
    private AtomicLong lastMetricUpdate = new AtomicLong();

    /**
     * This executor is used to schedule the writer task.
     */
    private ScheduledThreadPoolExecutor writerTaskExecutor;

    /**
     * This task will be executed after each update cycle. It will update the timestamp on the ZK-node that contains
     * the sendgraph for the topology this metrics consumer is configured for.
     */
    private Runnable dateUpdateTask;

    /**
     * This reference is used to cancel an already scheduled writer task, should more data have been received since we
     * started the timer the last time.
     */
    private Future<?> taskFuture;

    /**
     * This object is used for all communication with Zookeeper.
     */
    private static CuratorFramework zkClient;


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
        LOG.debug("Initializing the " + getClass().getCanonicalName());

        // open a connection to zookeeper
        this.zkClient = Stootils.getConfiguredZkClient(stormConf);

        this.zookeeperRootPath = getConfiguredMetricsZookeeperRootPath(stormConf) + "/" + context.getStormId();

        // create the root path in zookeeper
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(this.zookeeperRootPath);
        } catch (Exception e) {
            String errorMsg = "Couldn't create root folder in ZK for " + this.zookeeperRootPath;
            LOG.error(errorMsg);
            errorReporter.reportError(new RuntimeException(errorMsg, e));
        }

        this.updateTimeoutSecs = DEFAULT_SCHEDULING_METRIC_TIMEOUT_SECS;
        if (stormConf.containsKey(CONF_SCHEDULING_METRIC_TIMEOUT_SECS)) {
            this.updateTimeoutSecs = Utils.getInt(stormConf.get(CONF_SCHEDULING_METRIC_TIMEOUT_SECS));
        }

        this.dateUpdateTask = new Runnable() {
            @Override
            public void run() {
                byte[] lastUpdateAsByteArray = null;
                LOG.trace("Updating date of last update in ZK to {}", new Date(lastMetricUpdate.get()));

                lastUpdateAsByteArray = ByteBuffer.allocate(8).putLong(lastMetricUpdate.get()).array();

                try {
                    zkClient.setData().forPath(zookeeperRootPath, lastUpdateAsByteArray);
                } catch (Exception e) {
                    String errorMsg = "Couldn't write update timetamp into Zookeeper";
                    LOG.error(errorMsg);
                }


            }
        };
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        int taskId;

        taskId = taskInfo.srcTaskId;

        LOG.trace("Handling datapoints for task {}", taskId);
        for (DataPoint dp : dataPoints) {
            if (SchedulingMetricsCollectionHook.METRIC_EMITTED_MESSAGES.equals(dp.name)) {
                LOG.trace("Datapoint of task {}: {}", taskId, dp.value.toString());
                for (Map.Entry<Integer, AtomicLong> toTaskCount : ((Map<Integer, AtomicLong>) dp.value).entrySet()) {
                    // write the data into zk using a path constructed as follows:
                    // {fromTaskID}-{toTaskID}=count-as-byte-array
                    final String path = String.format("%s/%d-%d", this.zookeeperRootPath, taskId, toTaskCount.getKey());
                    final byte[] count = ByteBuffer.allocate(8).putLong(Long.valueOf(toTaskCount.getValue().get())).array();
                    try {
                        zkClient.checkExists().inBackground(new BackgroundCallback() {
                            @Override
                            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                                zkClient.setData().forPath(path, count);
                                lastMetricUpdate.set(System.currentTimeMillis());

                                // Reset the current timeout counter and re-schedule the date updater task.
                                if (taskFuture == null || taskFuture.cancel(false)) {
                                    taskFuture = writerTaskExecutor.schedule(dateUpdateTask, updateTimeoutSecs,
                                            TimeUnit.SECONDS);
                                } else {
                                    LOG.warn("Could not (re-) schedule the date update task.");
                                }
                            }
                        }).forPath(path);

                    } catch (Exception e) {
                        LOG.error("Could not write send graph to zookeeper", e);
                    }
                }
            }
        }
    }

    @Override
    public void cleanup() {
        this.zkClient.close();
    }
}
