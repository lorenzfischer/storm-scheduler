package ch.uzh.ddis.stools.topos;/* TODO: License */

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.stools.monitoring.MonitoringMetricsToGraphiteWriter;
import ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * This class creates and submits a parallel topology to the cluster.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class ParallelTopology {

    private final static Logger LOG = LoggerFactory.getLogger(ParallelTopology.class);

    @Option(name="--help", aliases={"-h"}, usage="print help message")
    private boolean help = false;

    @Option(name="--parallelism", aliases={"-p"}, metaVar="PARALLELISM",
            usage="number of spouts/bolts to generate on each level")
    private int parallelism = 1;

    @Option(name="--depth", aliases={"-d"}, metaVar="DEPTH",
            usage="number of bolts to concatenate to each other.")
    private int depth = 1;

    @Option(name="--local", aliases={"-l"}, usage="Run on local cluster")
    private boolean local = false;

    @Option(name="--nimbus", aliases={"-n"}, metaVar="NIMBUS",
            usage="thrift connection to nimbus (e.g. 192.168.1.20:6627)")
    private String nimbusServer;

    @Option(name="--workers", aliases={"-w"}, metaVar="WORKERS",
            usage="number of workers to assign to the topology")
    private int numWorkers = 1;

    @Option(name="--maxSpoutPending", aliases={"-msp"}, metaVar="MSP",
            usage="the maximum number of pending tuples in each worker")
    private int maxSpoutPending = 100*1000;


    public void realMain(String... args) {

        // parse the arguments
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            this.help = true;
        }

        if(this.help) {
            String desc;

            desc = "This class generates a topology which one spout and one bolt by default. If you set the " +
                    "--parallelism option to anything > 1, there will be as many spouts and bolts. Each spout emits " +
                    "a constant (but unique) value. The connection between the spout and the bolt is grouped on the " +
                    "message value, meaning that this topology should be very simple to schedule: just put every spout " +
                    "onto the same machine as the spout it receives messages from and you should have zero network " +
                    "costs.";
            System.err.println(desc);
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        TopologyBuilder b;
        Config conf;
        String topologyName = "parallel-topo";
        String previousName;


        b = new TopologyBuilder();
        LOG.trace("Adding Spout");
        b.setSpout("UuidSpout", new UuidSpout(), this.parallelism);
        previousName="UuidSpout";
        for (int i=0; i<this.depth; i++) {
            String boltName;

            boltName = "NothingBolt" + i;
            LOG.trace("Adding bolt {}", boltName);
            b.setBolt(boltName, new NothingBolt(), this.parallelism)
                    .fieldsGrouping(previousName, new Fields("message"));
            previousName = boltName;
        }

        conf = new Config();
        conf.setNumWorkers(this.numWorkers);
        //conf.setNumAckers(1);
//        conf.put("storm.scheduler", "ch.uzh.ddis.stools.scheduler.ZookeeperScheduler");
        conf.put("topology.auto.task.hooks",
                Arrays.asList( //
                        "ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook", //
                        "ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook") //
        );
        conf.registerMetricsConsumer(SchedulingMetricsToZookeeperWriter.class);
        conf.registerMetricsConsumer(MonitoringMetricsToGraphiteWriter.class);

        conf.put(MonitoringMetricsToGraphiteWriter.CONF_MONITORING_GRAPHITE_SERVER, "graphite.ifi.uzh.ch:2003");

        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.TRUE);

        conf.setMaxSpoutPending(this.maxSpoutPending);

        if (this.local) {
            LocalCluster localCluster;
            localCluster = new LocalCluster(conf);
            localCluster.submitTopology(topologyName, conf, b.createTopology());
        } else {

            if (this.nimbusServer != null) {
                String[] parts = this.nimbusServer.split(":");
                conf.put(Config.NIMBUS_HOST, parts[0]);
                conf.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(parts[1]));
            }

            try {
                StormSubmitter.submitTopology(topologyName, conf, b.createTopology());
            } catch (AlreadyAliveException e) {
                LOG.error("Could not submit topology.", e);
            } catch (InvalidTopologyException e) {
                LOG.error("Could not submit topology.", e);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        new ParallelTopology().realMain(args);
    }

}
