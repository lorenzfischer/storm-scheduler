/* TODO: License */
package ch.uzh.ddis.stools.scheduler;


import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import backtype.storm.utils.Utils;
import backtype.storm.utils.ZookeeperAuthInfo;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This is a dummy scheduler used for testing. It uses the even scheduler to schedule the topologies and only
 * adds a log line about the scheduling event taking place to the trace log.
 * <p/>
 * You can tell storm to use this scheduler by doing the following:
 * <ol>
 * <li>Put the jar containing this scheduler into <b>$STORM_HOME/lib on the nimbus</b> server.</li>
 * <li>Set the following configuration parameter: storm.scheduler: "ch.uzh.ddis.stools.scheduler.PartitioningScheduler"</li>
 * </ol>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class PartitioningScheduler implements IScheduler {

    private final static Logger LOG = LoggerFactory.getLogger(PartitioningScheduler.class);

    /** This default scheduler will be used if we cannot find any scheduling information. */
    private EvenScheduler evenScheduler;

    /**
     * This object is used for all communication with Zookeeper. We read the send graph from Zookeeper assuming
     * that {@link ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter} is registered and working.
     */
    private CuratorFramework zkClient;

    @Override
    public void prepare(Map stormConf) {
        String zkRootPath;

        LOG.debug("Initializing the PartitionScheduler");

        zkRootPath = SchedulingMetricsToZookeeperWriter.getConfiguredMetricsZookeeperRootPath(stormConf);
        this.evenScheduler = new EvenScheduler();

        // open a connection to zookeeper
        this.zkClient = Stootils.getConfiguredZkClient(stormConf);

        LOG.debug("Installing watcher for node {}", zkRootPath);
        try {
            this.zkClient.checkExists().usingWatcher(new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    LOG.trace("\n\n\nsomething happened in the path\n\n\n");
                }
            }).forPath(zkRootPath);
        } catch (Exception e) {
            LOG.error("Could install watcher on node " + zkRootPath, e);
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.trace("scheduling topologies...");


        this.evenScheduler.schedule(topologies, cluster);
    }

}
