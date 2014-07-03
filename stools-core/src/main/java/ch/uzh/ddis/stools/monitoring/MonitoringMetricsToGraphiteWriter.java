/* TODO: License */
package ch.uzh.ddis.stools.monitoring;


import backtype.storm.Config;
import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * This class consumes the metrics that are collected by the {@link ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook}
 * and {@link ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook} (if available) and writes them to a
 * graphite server (https://github.com/graphite-project/graphite-web).
 * <p/>
 * <p/>
 * You can configure it in the storm.yaml file as:
 * <pre>
 *   topology.metrics.consumer.register:
 *     - class: "ch.uzh.ddis.stools.monitoring.MonitoringMetricsToGraphiteWriter"
 *       parallelism.hint: 1
 * </pre>
 * <p/>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class MonitoringMetricsToGraphiteWriter implements IMetricsConsumer {

    /**
     * Graphite stores its metrics as paths. This base path will be prepended to the name of the topology (rather than
     * the ID of the topology) to form the path under which the metrics will be stored. So assuming the base path is set
     * to "{@value #DEFAULT_GRAPHITE_BASE_PATH}", the path for the "throughput" metric of topology "topoXYZ" would be
     * defined as:
     * <p/>
     * Example: {@value #DEFAULT_GRAPHITE_BASE_PATH}.topoXYZ.throughput
     * <p/>
     */
    public static final String DEFAULT_GRAPHITE_BASE_PATH = "stools";

    /**
     * If a property with this name can be found in the storm configuration object, the graphite base path will be set
     * to the value of the provided property.
     */
    public static final String CONF_MONITORING_GRAPHITE_BASE_PATH = "monitoring.graphite.base.path";

    /**
     * This is a mandatory property that needs to be set in the storm configuration map. It has to contain the
     * server and port of the graphite instance.
     * <p/>
     * Example:
     * {@value #CONF_MONITORING_GRAPHITE_SERVER}: "graphite.yourdomain.com:2003"
     */
    public static final String CONF_MONITORING_GRAPHITE_SERVER = "monitoring.graphite.server";

    private final static Logger LOG = LoggerFactory.getLogger(MonitoringMetricsToGraphiteWriter.class);

    /**
     * We will try to connect to graphite at most this many times, before giving up and waiting for the next cycle.
     * TODO: make configurable
     */
    private int maxConnectionAttempts = 5;

    /**
     * The path under which the metrics for the current topology will be stored in graphite.
     */
    private String graphitePath;

    /**
     * The connection information to the graphite server in the form "server:port".
     */
    private String graphiteConnection;

    /**
     * This object is used to write data to graphite.
     */
    private BufferedWriter graphiteWriter;

    /**
     * Only metrics whose name is contained in this map will be processed (sent to graphite).
     */
    private Map<String, AbstractAggregator> metricsToProcess;


    /**
     * This method computes and returns the path in Graphite, under which the metrics are stored. Because
     * this path is configurable through the property {@value #CONF_MONITORING_GRAPHITE_BASE_PATH} the storm
     * configuration map must be passed as an argument. It will contain the last "." of the path if necessary.
     * <p/>
     * Example: stools.topoXYZ.
     *
     * @param stormConf the storm configuration map.
     * @return the path in Graphite under which the metric data is stored.
     */
    public static String getConfiguredGraphitBasePath(Map stormConf) {
        String path;

        if (stormConf.containsKey(CONF_MONITORING_GRAPHITE_BASE_PATH)) {
            path = (String) stormConf.get(CONF_MONITORING_GRAPHITE_BASE_PATH);
        } else {
            path = DEFAULT_GRAPHITE_BASE_PATH;
        }

        path += "." + stormConf.get(Config.TOPOLOGY_NAME) + ".";

        return path;
    }

    /**
     * This method returns an object through which data can be written to graphite. If there is already a valid
     * socket connection to graphite open. That writer object will be returned. If the connection has not yet been
     * opened or has failed for some reason. A new connection will be opened. The method will try at most {@see #maxConnectionAttempts}
     * times before giving up and returning <code>null</code>.
     *
     * @return a writer object through which data can be written to graphite or null, if no connection could be established.
     */
    private BufferedWriter getGraphiteWriter() {
        if (this.graphiteWriter != null) {
            try {
                this.graphiteWriter.flush();
            } catch (IOException e) {
                LOG.warn("There was a problem with the connection to {}. Resetting and re-opening the connection: {}",
                        this.graphiteConnection, e.getMessage());
                try {
                    this.graphiteWriter.close();
                } catch (IOException e2) {
                    LOG.warn("Even closing the connection to {} failed because: {}",
                            this.graphiteConnection, e2.getMessage());
                }
                this.graphiteWriter = null;
            }
        }

        if (this.graphiteWriter == null) {
            int connectionAttempt;
            String[] hostPort;
            String host;
            int port;


            connectionAttempt = this.maxConnectionAttempts;
            hostPort = this.graphiteConnection.split(":");

            if (hostPort.length != 2) {
                throw new IllegalArgumentException("The graphite connection string must be of the form 'host:port'");
            }

            host = hostPort[0];
            port = Integer.parseInt(hostPort[1]);

            while (this.graphiteWriter == null && connectionAttempt-- > 0) {
                LOG.trace("Attempting to connect to {} ...", this.graphiteConnection);

                try {

                    Socket socket = new Socket(host, port);
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    this.graphiteWriter = new BufferedWriter(out);
                } catch (IOException e) {
                    LOG.warn("Failed to connect to the graphite server at {} because: {}",
                            this.graphiteConnection, e.getMessage());
                }
            }
        }

        return this.graphiteWriter;
    }

    @Override
    public void prepare(Map stormConf, Object registrationArgument, TopologyContext context,
                        final IErrorReporter errorReporter) {

        LOG.info("Initializing the " + getClass().getCanonicalName());

        this.graphitePath = getConfiguredGraphitBasePath(stormConf);
        if (!stormConf.containsKey(CONF_MONITORING_GRAPHITE_SERVER)) {
            throw new RuntimeException("Missing graphite configuration. You need to specify the server and the port" +
                    "under which the graphite server can be reached. Example: " + CONF_MONITORING_GRAPHITE_SERVER
                    + "=graphite.yourdomain.com:2003");
        }
        this.graphiteConnection = (String) stormConf.get(CONF_MONITORING_GRAPHITE_SERVER);

        getGraphiteWriter();

        this.metricsToProcess = new HashMap<>();
        this.metricsToProcess.put(MonitoringMetricsCollectionHook.METRIC_COMPLETE_LATENCY, new AverageAggregator());
        this.metricsToProcess.put(MonitoringMetricsCollectionHook.METRIC_THROUGHPUT, new SumAggregator());
        this.metricsToProcess.put(MonitoringMetricsCollectionHook.METRIC_WORKER_CPU_LOAD, new SumAggregator());
        this.metricsToProcess.put(MonitoringMetricsCollectionHook.METRIC_WORKER_NETWORK_BYTES, new SumAggregator());
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        BufferedWriter writer;

        writer = getGraphiteWriter();
        if (writer != null) {
            long timestamp;
            int taskId;

            taskId = taskInfo.srcTaskId;
            timestamp = System.currentTimeMillis() / 1000; // unix timestamps are in seconds

            LOG.trace("Handling datapoints for task {}", taskId);
            for (DataPoint dp : dataPoints) {

                if (this.metricsToProcess.containsKey(dp.name)) {
                    LOG.trace("Datapoint {} of task {}: {}", new Object[]{dp.name, taskId, dp.value.toString()});

                    Long value = this.metricsToProcess.get(dp.name).update(taskId, (Long) dp.value);

                    // prevent excessive writing to graphite by only writing the value to graphite when the task with
                    // ID "1" or if the metric contains the string "worker" gets updated. Metrics that are only
                    // collected once per worker and if the sending task happens to not be a task with ID "1" we
                    // wouldn't write anything to graphite otherwise.
                    if (taskId == 1 || dp.name.toLowerCase().contains("worker")) {
                        try {
                            String line;

                            line = String.format("%s %d %d", this.graphitePath + dp.name, value, timestamp);
                            LOG.trace("Writing to graphite: {} ", line);
                            writer.write(line);
                            writer.newLine();

                        } catch (IOException e) {
                            LOG.warn("Error while writing data to graphite: ", e);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void cleanup() {
        if (this.graphiteWriter != null) {
            try {
                this.graphiteWriter.close();
            } catch (IOException e) {
                LOG.warn("Error while closing the connection ot graphite: ", e);
            }
        }
    }
}
