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
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import ch.uzh.ddis.stools.monitoring.LoggingMetricsConsumer;
import ch.uzh.ddis.stools.monitoring.MonitoringMetricsToGraphiteWriter;
import ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.spout.RichSpoutBatchExecutor;

import java.util.Arrays;

/**
 * Almost the same as Paralleltopo
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class TridentPayloadTopology {

    private final static Logger LOG = LoggerFactory.getLogger(ParallelTopology.class);

    @Option(name = "--help", aliases = {"-h"}, usage = "print help message")
    private boolean help = false;

    @Option(name = "--parallelism", aliases = {"-p"}, metaVar = "PARALLELISM",
            usage = "number of spouts/bolts to generate on each level")
    private int parallelism = 1;

    @Option(name = "--depth", aliases = {"-d"}, metaVar = "DEPTH",
            usage = "number of bolts to concatenate to each other.")
    private int depth = 1;

    @Option(name = "--local", aliases = {"-l"}, usage = "Run on local cluster")
    private boolean local = false;

    @Option(name = "--nimbus", aliases = {"-n"}, metaVar = "NIMBUS",
            usage = "thrift connection to nimbus (e.g. 192.168.1.20:6627)")
    private String nimbusServer = null;

    @Option(name = "--graphite", aliases = {"-g"}, metaVar = "NIMBUS",
            usage = "connection to the graphite server (e.g. 192.168.1.20:2003)")
    private String graphiteServer = "graphite.ifi.uzh.ch:2003";

    @Option(name = "--workers", aliases = {"-w"}, metaVar = "WORKERS",
            usage = "number of workers to assign to the topology")
    private int numWorkers = 1;

    @Option(name = "--maxSpoutPending", aliases = {"-msp"}, metaVar = "MSP",
            usage = "the maximum number of pending tuples in each worker")
    private int maxSpoutPending = 100 * 1000;

    @Option(name = "--messageTimeoutSecs", aliases = {"-mts"}, metaVar = "MTS",
            usage = "the maximum number of seconds we wait for a message to process before failing it")
    private int messageTimeoutSecs = 100;

    @Option(name = "--numAckers", aliases = {"-a"}, metaVar = "A",
            usage = "the number of acker tasks to start.")
    private int numAckers = 0;

    @Option(name = "--batchSize", aliases = {"-bs"}, metaVar = "BATCHSIZE",
            usage = "the size of each trident batch.")
    private int batchSize = 100 * 1000;

    @Option(name="--payloadFactor", aliases={"-f"}, metaVar="PF",
            usage="a multiplier that defines the size of the payload.")
    private int payloadFactor = 100;

    public void realMain(String... args) {

        // parse the arguments
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            // parse the arguments.
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.getMessage());
            this.help = true;
        }

        if (this.help) {
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

        storm.trident.TridentTopology topology;
        StormTopology stormTopo;
        Config conf;
        String topologyName = "trident-payload-topo";
        String previousName;


        LOG.trace("Adding Spout");
        topology = new storm.trident.TridentTopology();

        Stream s = topology.newStream("UuidSpout", new UuidPayloadSpout(true, this.payloadFactor)).name("UuidSpout").parallelismHint(this.parallelism);
        previousName = "keyfield";

        for (int i = 0; i < this.depth; i++) {
            String newFieldName;

            newFieldName = "field" + i;
            LOG.trace("Adding level {} with new field {}", i, newFieldName);
            s = s//
                    .partitionBy(new Fields(previousName)) //
                    .name(previousName)
                    .each(new Fields(previousName), new IncreaseHashByOneFunction(this.parallelism), new Fields(newFieldName)) //
                    .project(new Fields(newFieldName, "payload"))
                    .parallelismHint(this.parallelism);
            previousName = newFieldName;
        }

        stormTopo = topology.build();

        conf = new Config();

        // set batch - size of spout
        conf.put(RichSpoutBatchExecutor.MAX_BATCH_SIZE_CONF, this.batchSize);


        conf.setNumWorkers(this.numWorkers);
        if (this.numAckers > 0) {
            conf.setNumAckers(this.numAckers);
        }

        conf.put("topology.auto.task.hooks",
                Arrays.asList( //
                        "ch.uzh.ddis.stools.scheduler.SchedulingMetricsCollectionHook", //
                        "ch.uzh.ddis.stools.monitoring.MonitoringMetricsCollectionHook") //
        );
        conf.registerMetricsConsumer(SchedulingMetricsToZookeeperWriter.class);
        conf.registerMetricsConsumer(MonitoringMetricsToGraphiteWriter.class);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1); // write metrics into metrics.log

        conf.put(MonitoringMetricsToGraphiteWriter.CONF_MONITORING_GRAPHITE_SERVER, this.graphiteServer);

        conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, Boolean.TRUE);

        conf.setMaxSpoutPending(this.maxSpoutPending);
        conf.setMessageTimeoutSecs(this.messageTimeoutSecs);

        if (this.local) {
            LocalCluster localCluster;
            localCluster = new LocalCluster(conf);
            localCluster.submitTopology(topologyName, conf, stormTopo);
        } else {

            if (this.nimbusServer != null) {
                String[] parts = this.nimbusServer.split(":");
                conf.put(Config.NIMBUS_HOST, parts[0]);
                conf.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(parts[1]));
            }

            try {
                StormSubmitter.submitTopology(topologyName, conf, stormTopo);
            } catch (AlreadyAliveException e) {
                LOG.error("Could not submit topology.", e);
            } catch (InvalidTopologyException e) {
                LOG.error("Could not submit topology.", e);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        new TridentPayloadTopology().realMain(args);
    }

}