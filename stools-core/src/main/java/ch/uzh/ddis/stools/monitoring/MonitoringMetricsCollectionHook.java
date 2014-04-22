/* TODO: License */
package ch.uzh.ddis.stools.monitoring;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.*;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This hook will install metrics to collect statistics on various attributes of the storm topology this
 * hook is configured for. All metrics are measured on a "per second" basis, meaning we divide all metric
 * values by the number of seconds of the currently configured interval ({@see DEFAULT_INTERVAL_SECS}).
 * <p/>
 * <p/>
 * You can configure it in the storm.yaml file as:
 * <pre>
 *   topology.auto.task.hooks:
 *     - "ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook"
 * </pre>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class MonitoringMetricsCollectionHook implements ITaskHook {

    private static final long serialVersionUID = 1L;

    /**
     * The name for the metric that measures the latency of tuples, i.e. the time it took from when a tuple has been
     * emitted from a spout until the tuple tree was committed (fully acked).
     */
    public static final String METRIC_COMPLETE_LATENCY = "complete_latency";

    /**
     * The name for the metric that measures the throughput of tuples, i.e. the number of tuples the spouts of this
     * topology emit per second.
     */
    public static final String METRIC_THROUGHPUT = "throughput";

//    /**
//     * The name for the metric that measures the length of the longest queue. Of all bolts.
//     */
//    public static final String METRIC_LONGEST_QUEUE_LENGTH = "longest_queue_length";


    /**
     * The intervalSecs in which the metrics will be collected.
     */
    public static final int DEFAULT_INTERVAL_SECS = 5;

    /**
     * The intervalSecs configured in this property will be used for counting (and resetting) the metrics collected
     * by this hook. If this is not set in the storm properties the default value of {@value #DEFAULT_INTERVAL_SECS}
     * will be used. Be careful not to set this value too low (smaller than the default value), as this could lead
     * to incomplete send data being written to ZK.
     */
    public static final String CONF_MONITORING_METRICS_INTERVAL = "monitoring.metrics.interval.secs";

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringMetricsCollectionHook.class);


    /**
     * The metric value for completeLatencySum.
     */
    private transient AtomicLong completeLatencySum = new AtomicLong();

    /**
     * We count the number of ackedMessages in each cycle, so we can compute the complete latency per acked message.
     */
    private transient AtomicLong ackedMessages = new AtomicLong();

    /**
     * The metric value for throughput.
     */
    private transient AtomicLong emittedMessages = new AtomicLong();

    @Override
    public void prepare(Map conf, final TopologyContext context) {
        final int intervalSecs;
        Map<String, AtomicLong> metrics;

        LOG.info("Initializing metrics hook for task {}", context.getThisTaskId());

        if (conf.containsKey(CONF_MONITORING_METRICS_INTERVAL)) {
            intervalSecs = Utils.getInt(conf.get(CONF_MONITORING_METRICS_INTERVAL)).intValue();
        } else {
            intervalSecs = DEFAULT_INTERVAL_SECS;
        }

        context.registerMetric(METRIC_COMPLETE_LATENCY, new IMetric() {
            @Override
            public Object getValueAndReset() {
                Long currentValue;

                currentValue = completeLatencySum.longValue() / Math.max(ackedMessages.longValue(), 1);
                currentValue = currentValue / intervalSecs;  // this will floor the value
                completeLatencySum.set(0);
                ackedMessages.set(0);

                LOG.trace("Reset metric value {} for task {} and returning: {}",
                        new Object[]{METRIC_COMPLETE_LATENCY, context.getThisTaskId(), currentValue.toString()});

                return currentValue;
            }

        }, intervalSecs); // call every n seconds


        context.registerMetric(METRIC_THROUGHPUT, new IMetric() {
            @Override
            public Object getValueAndReset() {
                Long currentValue;

                currentValue = emittedMessages.longValue();
                currentValue = currentValue / intervalSecs;  // this will floor the value
                emittedMessages.set(0);

                LOG.trace("Reset metric value {} for task {} and returning: {}",
                        new Object[]{METRIC_THROUGHPUT, context.getThisTaskId(), currentValue.toString()});

                return currentValue;
            }

        }, intervalSecs); // call every n seconds
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {
        // in order to compute the throughput, we just count how many messages were emitted by this task
        this.emittedMessages.incrementAndGet();
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
        if (info != null && info.completeLatencyMs != null) {
            this.completeLatencySum.getAndAdd(info.completeLatencyMs);
        }
        this.ackedMessages.incrementAndGet();
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {

    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {

    }

    @Override
    public void boltAck(BoltAckInfo info) {

    }

    @Override
    public void boltFail(BoltFailInfo info) {

    }

}
