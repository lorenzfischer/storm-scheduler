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

package ch.uzh.ddis.stools.topos;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.messaging.local;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook;
import ch.uzh.ddis.stools.monitoring.MonitoringMetricsToGraphiteWriter;
import ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter;

import java.util.Arrays;
import java.util.HashMap;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class StatsCollectionTester {


    public static void main(String... args) {
        final int parallelismHint = 3;
        TopologyBuilder b;
        Config conf;
        LocalCluster localCluster;

        b = new TopologyBuilder();
        b.setSpout("RandomSpout", new RandomSpout(true), parallelismHint);
        b.setBolt("NothingBolt1", new NothingBolt(true), parallelismHint)
                .shuffleGrouping("RandomSpout");
        b.setBolt("NothingBolt2", new NothingBolt(true), parallelismHint)
                .shuffleGrouping("NothingBolt1");

        conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.put("storm.scheduler", "ch.uzh.ddis.stools.scheduler.ZookeeperScheduler");
        conf.put("topology.auto.task.hooks",
                Arrays.asList("ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook",
                        "ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook"));
        conf.registerMetricsConsumer(SchedulingMetricsToZookeeperWriter.class);

        conf.registerMetricsConsumer(MonitoringMetricsToGraphiteWriter.class);
        conf.put(MonitoringMetricsToGraphiteWriter.CONF_MONITORING_GRAPHITE_SERVER, "tentacle.ifi.uzh.ch:2003");

        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.TRUE);

        localCluster = new LocalCluster(conf);
        localCluster.submitTopology("test-topo", conf, b.createTopology());


    }

}
