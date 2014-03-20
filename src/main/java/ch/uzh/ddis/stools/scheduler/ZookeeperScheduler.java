/* TODO: License */
package ch.uzh.ddis.stools.scheduler;


import backtype.storm.scheduler.*;
import com.netflix.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This scheduler reads the schedule from Zookeeper. It uses the even scheduler to schedule all the topologies
 * for which is doesn't find a schedule in Zookeeper.
 * <p/>
 * You can tell storm to use this scheduler by doing the following:
 * <ol>
 * <li>Put the jar containing this scheduler into <b>$STORM_HOME/lib on the nimbus</b> server.</li>
 * <li>Set the following configuration parameter: storm.scheduler: "ch.uzh.ddis.stools.scheduler.ZookeeperScheduler"</li>
 * </ol>
 * <p/>
 * The schedules are expected to be written into Zookeeper znodes having the names of the topology-ids under  either
 * {@link #DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH} (value = {@value #DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH}) or a path that
 * is configured as a property in the storm config object {@link #CONF_SCHEDULING_SCHEDULES_ZK_PATH} (value =
 * {@value #CONF_SCHEDULING_SCHEDULES_ZK_PATH}).
 * <p/>
 * <b>Please Note #1:</b> This scheduler assumes that there is task per executor thread. If there are multiple tasks
 * assigned per executor, the scheduler will throw an exception. However, multiple executor threads can be assigned
 * to one single worker.
 * <p/>
 * <b>Please Note #2:</b> This scheduler will ignore the configured number of workers per topology configured in
 * {@link backtype.storm.Config#TOPOLOGY_WORKERS} as it merely applies the configured schedule it finds in Zookeeper.
 * The partitions it finds configured will be applied, treating supervisors as partitions. This means that the tasks
 * that are assigned to a partition will be distributed among all workers that are running on a given supervisor.
 * This is easiest to reason about if there is exactly one supervisor per host and one worker (slot) per supervisor.
 * If multiple workers per supervisor, the tasks will be distributed evenly among all workers of each supervisor.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class ZookeeperScheduler implements IScheduler {

    /**
     * The schedule are expected to be written into nodes under this path. The schedules will have one line per
     * task, and on each line the partition the task should be assigned to. The partitions are numbered from [0,P), where
     * P stands for the number of partitions (number of workers requested by the topology).
     * <p/>
     * <p/>
     * Example: /stools/scheduler/schedules/topoXYZ
     */
    public static final String DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH = "/stools/scheduler/schedule";

    /**
     * If a property with this name can be found in the storm configuration object the root path
     * in zookeeper will be set to the value of the provided property.
     */
    public static final String CONF_SCHEDULING_SCHEDULES_ZK_PATH = "scheduling.schedules.zk.path";

    private final static Logger LOG = LoggerFactory.getLogger(ZookeeperScheduler.class);

    /**
     * This default scheduler will be used if we cannot find any scheduling information.
     */
    private EvenScheduler evenScheduler;

    /**
     * This object is used for all communication with Zookeeper. We read the send graph from Zookeeper assuming
     * that {@link ch.uzh.ddis.stools.scheduler.SchedulingMetricsToZookeeperWriter} is registered and working.
     */
    private CuratorFramework zkClient;

    /**
     * The root path under which the schedules for each topology is expected to be stored.
     */
    private String zkRootPath;

    /**
     * We store all schedules for all topologies in this map, so we can decide if the schedule that we read from
     * Zookeeper is different from the one we have currently employed.
     */
    private Map<String, String> scheduleMap;

    /**
     * This method computes and returns the path in Zookeeper under which the schedules are expected to be stored.
     * Because this path is configurable through the property {@value #CONF_SCHEDULING_SCHEDULES_ZK_PATH} the storm
     * configuration map must be passed as an argument.
     * <p/>
     * For each topology, there will be a sub node in this path, which in turn contains schedule as that znode's data.
     * For example schedule for topology test-topo-1-1394729310 would be stored in the znode:<br/>
     * <pre>
     * {rootPath}/test-topo-1-1394729310
     * </pre>
     *
     * @param stormConf the storm configuration map.
     * @return the path in Zookeeper under which the schedule should be stored.
     */
    public static String getConfiguredSchedulesZookeeperRootPath(Map stormConf) {
        String path;

        if (stormConf.containsKey(CONF_SCHEDULING_SCHEDULES_ZK_PATH)) {
            path = (String) stormConf.get(CONF_SCHEDULING_SCHEDULES_ZK_PATH);
        } else {
            path = DEFAULT_SCHEDULING_SCHEDULES_ZK_PATH;
        }

        return path;
    }

    @Override
    public void prepare(Map stormConf) {
        LOG.debug("Initializing the ZookeeperScheduler");

        this.scheduleMap = new HashMap<>();
        this.zkRootPath = getConfiguredSchedulesZookeeperRootPath(stormConf);
        this.evenScheduler = new EvenScheduler(); // default scheduler to use
        this.zkClient = Stootils.getConfiguredZkClient(stormConf); // open a connection to zookeeper
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        Map<String, TopologyDetails> unscheduledTopologies; // key = topology-id

        LOG.debug("scheduling topologies...");

        unscheduledTopologies = new HashMap<>();

        /*
         This method will be called every couple of seconds (TODO: find out how often), so we don't need to attach
         a watcher. We just read the schedule from Zookeeper and check if it is different from the schedule we have
         currently in place. If the schedule is new, we re-schedule the topology.
         */
        for (TopologyDetails topology : topologies.getTopologies()) {
            String topologyId;
            String topologyPath;
            String schedule;

            topologyId = topology.getId();
            topologyPath = this.zkRootPath + "/" + topologyId;

            LOG.trace("Scheduling topology {}", topologyId);

            try {
                LOG.trace("Trying to read schedule from zookeeper at {}", topologyPath);
                schedule = new String(this.zkClient.getData().forPath(topologyPath));
            } catch (KeeperException.NoNodeException e) {
                LOG.warn("Couldn't not find a schedule on zookeeper for topology {}", topologyId);
                unscheduledTopologies.put(topologyId, topology);
                continue; // continue with next topology, maybe it will work when we are called next.
            } catch (Exception e) {
                LOG.warn("Error while reading schedule from zookeeper at " + topologyPath, e);
                unscheduledTopologies.put(topologyId, topology);
                continue; // continue with next topology, maybe it will work when we are called next.
            }

            // is the schedule we read from zk actually new?
            if (!this.scheduleMap.containsKey(topologyId) || this.scheduleMap.get(topologyId).equals(schedule)) {
                String[] taskToWorkerIndexString;
                int[] taskToWorkerIndex;
                List<Set<ExecutorDetails>> tasksOfPartition; // the index is the partition/worker id
                int numWorkers;
                LinkedList<SupervisorDetails> availableSupervisors;

                // the schedule is one number per line, each being the index of the partition the task for the given
                // line should be assigned to. The line number is the task index. the partition ids are 0-based
                // because the METIS partitionings use that format
                numWorkers = 0;
                taskToWorkerIndexString = schedule.split("\n");
                taskToWorkerIndex = new int[taskToWorkerIndexString.length];
                for (int i=0; i<taskToWorkerIndex.length; i++) {
                    int worker = Integer.parseInt(taskToWorkerIndexString[i]); // worker = partition
                    taskToWorkerIndex[i] = worker;
                    numWorkers = Math.max(numWorkers, worker);
                }

                LOG.trace("Schedule is considering {} tasks that have to be scheduled to {} workers",
                        taskToWorkerIndex.length, numWorkers);

                // initialize the assignment collections
                tasksOfPartition = new ArrayList<>();
                for (int i=0; i<numWorkers; i++) {
                    tasksOfPartition.add(new HashSet<ExecutorDetails>());
                }

                for (ExecutorDetails executor : topology.getExecutors()) {
                    int taskId;

                    if (executor.getStartTask() != executor.getEndTask()) {
                        String errorMsg = String.format("Cannot handle executors with more than one task. Found" +
                                " an executor with starTask=%d and endTask=%d for component %s.",
                                executor.getStartTask(), executor.getEndTask(),
                                topology.getExecutorToComponent().get(executor));
                        throw new IllegalArgumentException(errorMsg);
                    }

                    // taskIds are 1-based
                    // if you get exception around here for taskIds not matching, maybe task id's are not always 1-based ;-)
                    taskId = executor.getStartTask();
                    LOG.trace("handling task {}", taskId);
                    tasksOfPartition.get(taskToWorkerIndex[taskId]).add(executor);
                }


                /*
                    The assignment process works as follows:
                    1.  create a linkedlist from all supervisors by putting all the supervisors that have currently no
                        workers assigned to the beginning of the list and partially busy supervisors at the end of the
                        list. Blacklisted hosts will be ignored.
                    2.  while there are still un-assigned partitions:
                    2.1 select next supervisor
                    2.2 get a list of all workers on the selected supervisor
                    2.3 distribute all tasks of that partition among the supervisors the given host
                 */
                availableSupervisors = new LinkedList<>();
                for (SupervisorDetails supervisor : cluster.getSupervisors().values()) {
                    if (!cluster.isBlackListed(supervisor.getId())) {
                        if (cluster.getUsedPorts(supervisor).size() > 0) {
                            availableSupervisors.addLast(supervisor); // has used ports
                        } else {
                            availableSupervisors.addFirst(supervisor); // totally unoccupied supervisor
                        }
                    }
                }
                // now assign the work
                for (Set<ExecutorDetails> task : tasksOfPartition) {
                    SupervisorDetails supervisor;

                    if (availableSupervisors.size() == 0) {
                        LOG.error("Could not schedule topology {} " +
                                "because there are no more supervisors available", topologyId);
                    }

                    supervisor = availableSupervisors.remove()

                    for  (SupervisorDetails selectedSupervisor : availableSupervisors) {

                        if (cluster.getBlacklistedHosts().contains(selectedSupervisor.getHost())) {
                            if (selectedSupervisor != lastInList) {
                                // ignore this supervisor and take the next one
                            }
                        }
                    }
                }
            }

        }

        if (unscheduledTopologies.size() > 0) {
            LOG.debug("Scheduling {} remaining topologies using the even scheduler ...", unscheduledTopologies.size());
            this.evenScheduler.schedule(new Topologies(unscheduledTopologies), cluster);
        }

    }

}
