/* TODO: License */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.messaging.local;
import backtype.storm.testing.TestWordCounter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
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
        b.setSpout("RandomSpout", new RandomSpout(), parallelismHint);
        b.setBolt("NothingBolt1", new NothingBolt(), parallelismHint)
                .shuffleGrouping("RandomSpout");
        b.setBolt("NothingBolt2", new NothingBolt(), parallelismHint)
                .shuffleGrouping("NothingBolt1");

        conf = new Config();
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.put("storm.scheduler", "ch.uzh.ddis.stools.scheduler.ZookeeperScheduler");
        conf.put("topology.auto.task.hooks",
                Arrays.asList("ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook"));
        conf.registerMetricsConsumer(SchedulingMetricsToZookeeperWriter.class);
        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.TRUE);

        localCluster = new LocalCluster(conf);
        localCluster.submitTopology("test-topo", conf, b.createTopology());


    }

}
