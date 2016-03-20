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

package ch.uzh.ddis.stools.monitoring;

import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.*;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import ch.uzh.ddis.stools.utils.NetStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * This metric is emitted once per worker (VM) and reflects the load of the cpu.
     */
    public static final String METRIC_WORKER_CPU_LOAD = "worker_cpu_load";

    /**
     * This metric is emitted once per worker (VM) and reflects the number of bytes sent + received of the network.
     */
    public static final String METRIC_WORKER_NETWORK_BYTES = "worker_network_bytes";

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
     * On each worker (VM), we register one metric to collect information about the machines CPU
     * and network interface usage. In order to prevent multiple of these collectors being started, we
     * write the taskID of the first task initiating this metric into this static variable and skip
     * the registration for all remaining tasks.
     * <p/>
     * For as long as there is only one worker per supervisor, this allows us to measure cpu load and
     * network load per cluster machine.
     */
    private static AtomicInteger workerMonitoringTaskId = new AtomicInteger(Integer.MIN_VALUE);


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

        /*
            we test if the workerMonitoringTaskId is still set to Integer.MIN_VALUE and if not, set it to the id of the
            task we're initiating here. As the workerMonitoringTaskId is updated atomically, there can always only be one
            such workerMonitor registered per VM (the workerMonitoringTaskId is a static variable).
         */
        if (workerMonitoringTaskId.compareAndSet(Integer.MIN_VALUE, context.getThisTaskId())) {

            // register one metric to measure cpu load
            context.registerMetric(METRIC_WORKER_CPU_LOAD, new IMetric() {
                private OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

                @Override
                public Object getValueAndReset() {
                    return Double.valueOf(osBean.getSystemLoadAverage() * 100).longValue();
                }

            }, intervalSecs); // call every n seconds


            // register one metric to measure network load
            context.registerMetric(METRIC_WORKER_NETWORK_BYTES, new IMetric() {
                private Long lastValue = 0L;

                @Override
                public Object getValueAndReset() {
                    long metric = 0L;
                    try {
                        long currentValue;

                        currentValue = NetStat.getTotalTransmittedBytes();
                        if (lastValue > 0) {
                            metric = (currentValue - lastValue) / intervalSecs;
                        }
                        lastValue = currentValue;

                    } catch (IOException e) {
                        LOG.warn("Error occurred while trying to get network load.", e);
                    }

                    return metric;
                }

            }, intervalSecs); // call every n seconds
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {
        //System.out.println(Thread.currentThread().getName());
        /*
          This method is executed for ALL emitted messages. No matter if it is
          a spout, bolt, or system bolt (acker).
         */
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
