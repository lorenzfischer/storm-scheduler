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

import backtype.storm.metric.api.IMetricsConsumer;
import backtype.storm.task.IErrorReporter;
import backtype.storm.task.TopologyContext;
import ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is basically a copy of {@link backtype.storm.metric.LoggingMetricsConsumer} with the difference that certain
 * metrics are except from being logged.
 * <p/>
 * To use, add this to your topology's configuration:
 * conf.registerMetricsConsumer(ch.uzh.ddis.stools.monitoring.LoggingMetricsConsumer.class, 1);
 * <p/>
 * Or edit the storm.yaml config file:
 * <p/>
 * topology.metrics.consumer.register:
 * - class: "ch.uzh.ddis.stools.monitoring.LoggingMetricsConsumer"
 * parallelism.hint: 1
 * <p/>
 * In order to channel the log lines from this logger into the metrics.log logfile, you need to add the following lines
 * to your logback.xml file:
 * <p/>
 * <logger name="ch.uzh.ddis.stools.monitoring.LoggingMetricsConsumer" additivity="false" >
 * <level value="INFO"/>
 * <appender-ref ref="METRICS"/>
 * </logger>
 * <p/>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class LoggingMetricsConsumer implements IMetricsConsumer {
    public static final Logger LOG = LoggerFactory.getLogger(LoggingMetricsConsumer.class);

    /** This set contains all metrics that we don't write into the metrics log. */
    private Set<String> extemptMetrics;

    /**
     * The id of the topology we're running in, so we can output this into the log.
     */
    private String stormId;

    @Override
    public void prepare(Map stormConf,
                        Object registrationArgument,
                        TopologyContext context,
                        IErrorReporter errorReporter) {
        this.stormId = context.getStormId();
        this.extemptMetrics = new HashSet<>();

        this.extemptMetrics.add(SchedulingMetricsCollectionHook.METRIC_EMITTED_MESSAGES);
    }

    static private String padding = "                                 ";

    @Override
    public void handleDataPoints(TaskInfo taskInfo, Collection<DataPoint> dataPoints) {
        StringBuilder sb = new StringBuilder();
//        String header = String.format("%d\t%15s:%-4d\t%-20s\t-11s\t%3d\t",
//                taskInfo.timestamp,
//                taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
//                this.stormId, // max 20 characters long
//                taskInfo.srcComponentId,
//                taskInfo.srcTaskId);
        String header = String.format("\t%d\t%15s:%-4d\t%-20s\t%-11s\t%3d\t",
                taskInfo.timestamp,
                taskInfo.srcWorkerHost, taskInfo.srcWorkerPort,
                this.stormId,
                taskInfo.srcComponentId,
                taskInfo.srcTaskId);
        sb.append(header);
        for (DataPoint p : dataPoints) {
            if (!this.extemptMetrics.contains(p.name)) {
                sb.delete(header.length(), sb.length());
                sb.append(p.name)
                        .append(padding).delete(header.length() + 33, sb.length()).append("\t")
                        .append(p.value);
                LOG.info(sb.toString());
            }
        }
    }

    @Override
    public void cleanup() {
    }
}