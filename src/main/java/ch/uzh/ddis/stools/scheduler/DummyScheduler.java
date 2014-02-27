/* TODO: License */
package ch.uzh.ddis.stools.scheduler;


import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This is a dummy scheduler used for testing. It uses the even scheduler to schedule the topologies and only
 * adds a log line about the scheduling event taking place to the trace log.
 *
 * You can tell storm to use this scheduler by doing the following:
 * <ol>
 *   <li>Put the jar containing this scheduler into <b>$STORM_HOME/lib on the nimbus</b> server.</li>
 *   <li>Set the following configuration parameter: storm.scheduler: "ch.uzh.ddis.stools.scheduler.DummyScheduler"</li>
 * </ol>
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class DummyScheduler implements IScheduler {

    private final static Logger LOG = LoggerFactory.getLogger(DummyScheduler.class);

    /** This default scheduler will be used if we cannot find any scheduling information. */
    private EvenScheduler evenScheduler;

    @Override
    public void prepare(Map conf) {
         this.evenScheduler = new EvenScheduler();
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.trace("scheduling topologies...");
        this.evenScheduler.schedule(topologies, cluster);
    }
}
