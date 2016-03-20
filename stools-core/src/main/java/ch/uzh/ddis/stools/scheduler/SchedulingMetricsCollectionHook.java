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

import backtype.storm.hooks.ITaskHook;
import backtype.storm.hooks.info.*;
import backtype.storm.metric.api.IMetric;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This hook will install metrics to collect statistics on the sending behavior of the topology this
 * hook is configured for.
 * <p/>
 * You can configure it in the storm.yaml file as:
 * <pre>
 *   topology.auto.task.hooks:
 *     - "ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook"
 * </pre>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class SchedulingMetricsCollectionHook implements ITaskHook {

    private static final long serialVersionUID = 1L;

    /**
     * The name for the metric that measures all emitted messages.
     */
    public static final String METRIC_EMITTED_MESSAGES = "emitted_messages";

    /**
     * The intervalSecs in which the metrics (the sendgraph) will be collected.
     */
    public static final int DEFAULT_INTERVAL_SECS = 15;

    /**
     * The intervalSecs configured in this property will be used for counting (and resetting) the metrics collected
     * by this hook. If this is not set in the storm properties the default value of {@value #DEFAULT_INTERVAL_SECS}
     * will be used. Be careful not to set this value too low (smaller than the default value), as this could lead
     * to incomplete send data being written to ZK.
     */
    public static final String CONF_SCHEDULING_METRICS_INTERVAL_SECS = "scheduling.metrics.interval.secs";

    private final static Logger LOG = LoggerFactory.getLogger(SchedulingMetricsCollectionHook.class);

    /**
     * A reference to the send graph of the task this hook is attached to.
     */
    private transient AtomicReference<Map<Integer, AtomicLong>> sendgraphRef;

    /**
     * Create an empty sendgraph map which will return 0L values for each non-existing entry. The semantics
     * of the entries in this map are 'how many messages have been sent from the task this hook is attached to
     * to other tasks in the storm task graph'. The source task id is therefore implicit.
     */
    private static Map<Integer, AtomicLong> createEmptySendgraphMap() {
        return new HashMap<Integer, AtomicLong>() {
            @Override
            public AtomicLong get(Object key) {
                AtomicLong result = super.get(key);
                if (result == null) {
                    result = new AtomicLong();
                    put((Integer) key, result);
                }
                return result;
            }

            @Override
            public String toString() {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<Integer, AtomicLong> entry : entrySet()) {
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(entry.getKey()).append(":").append(entry.getValue().get());
                }
                return sb.toString();
            }
        };
    }

    /**
     * Returns the configured interval in which metrics should be send to the metrics consumer.
     *
     * @param stormConf the configuration object to check.
     * @return the interval value in seconds.
     */
    public static int getConfiguredSchedulingIntervalSecs(Map stormConf) {
        int result;

        result = DEFAULT_INTERVAL_SECS;
        if (stormConf.containsKey(CONF_SCHEDULING_METRICS_INTERVAL_SECS)) {
            result = Utils.getInt(stormConf.get(CONF_SCHEDULING_METRICS_INTERVAL_SECS)).intValue();
        }

        return result;
    }

    @Override
    public void prepare(Map conf, final TopologyContext context) {
        if (context.getThisTaskId() < 0) {
            LOG.debug("Skipping installation of metrics hook for negative task id {}", context.getThisTaskId());
        } else {
            int intervalSecs;

            LOG.info("Initializing metrics hook for task {}", context.getThisTaskId());

            this.sendgraphRef = new AtomicReference<>();

            intervalSecs = getConfiguredSchedulingIntervalSecs(conf);

            /*
            * We register one metric for each task. The full send graph will then be built up in the metric
            * consumer.
            */
            context.registerMetric(METRIC_EMITTED_MESSAGES, new IMetric() {
                @Override
                public Object getValueAndReset() {
                    Map<Integer, AtomicLong> currentValue;

                    // don't reset sendgraph! todo: make this configurable
                    // currentValue = SchedulingMetricsCollectionHook.this.sendgraphRef.getAndSet(createEmptySendgraphMap());
                    currentValue = SchedulingMetricsCollectionHook.this.sendgraphRef.get();

                    LOG.trace("Reset values for task {} and returning: {}", context.getThisTaskId(), currentValue.toString());

                    return currentValue;
                }

            }, intervalSecs); // call every n seconds

            // put an empty send graph object.
            this.sendgraphRef.compareAndSet(null, createEmptySendgraphMap());

            // put a zero weight for the task at hand, so we have a complete send graph in the metrics. Without this
            // step, tasks that don't send or receive anything (for example the metrics-consumers) would not be
            // contained in the sendgraph. todo: change the schedule format to contain task=>partition assignements, so
            // we could do away with this workaround
            this.sendgraphRef.get().get(context.getThisTaskId()).set(0);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void emit(EmitInfo info) {
        if (this.sendgraphRef != null) {
            Map<Integer, AtomicLong> sendgraph;

            sendgraph = this.sendgraphRef.get();

            if (!info.stream.contains("__ack") &&          // don't measure ack messages
                    !info.stream.contains("__metrics")) {  // don't measure metrics messages
                if (sendgraph != null) {
                    for (Integer outTaskId : info.outTasks) {
                        sendgraph.get(outTaskId).incrementAndGet();
                    }
                }
            }
        }
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {

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
