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


import backtype.storm.scheduler.*;
import backtype.storm.tuple.Values;
import com.esotericsoftware.minlog.Log;
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
        with_next_topology:
        for (TopologyDetails topology : topologies.getTopologies()) {
            String topologyId;
            String topologyName;
            String topologyPath;
            String schedule;

            topologyId = topology.getId();
            topologyName = topology.getName();
            topologyPath = this.zkRootPath + "/" + topologyName;

            LOG.debug("Topo {}: Scheduling topology", topologyId);

            try {
                byte[] scheduleData;

                LOG.debug("Topo {}: Trying to read schedule from zookeeper at {}", topologyId, topologyPath);

                scheduleData = this.zkClient.getData().forPath(topologyPath);
                if (scheduleData == null) {
                    LOG.debug("Topo {}: The schedule on zookeeper was 'null'. Skipping topology.", topologyId);
                    unscheduledTopologies.put(topologyId, topology);
                    continue with_next_topology; // continue with next topology, maybe it will work when we are called next.
                }

                LOG.trace("Topo {}: Found schedule in zookeeper", topologyId);
                schedule = new String(scheduleData);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("Topo {}: Couldn't not find a schedule on zookeeper. Skipping topology.", topologyId);
                unscheduledTopologies.put(topologyId, topology);
                continue with_next_topology; // continue with next topology, maybe it will work when we are called next.
            } catch (Exception e) {
                LOG.warn("Error while reading schedule from zookeeper at " + topologyPath, e);
                unscheduledTopologies.put(topologyId, topology);
                continue with_next_topology; // continue with next topology, maybe it will work when we are called next.
            }

            // is the schedule we read from zk actually new?
            if (!this.scheduleMap.containsKey(topologyId) || !this.scheduleMap.get(topologyId).equals(schedule)) {
                SchedulerAssignment existingAssignment;
                String[] taskToWorkerIndexString;
                int[] taskToWorkerIndex;
                List<Set<ExecutorDetails>> tasksOfPartition; // the index is the partition/worker id
                int numWorkers;
                LinkedList<SupervisorDetails> availableSupervisors;
                LOG.debug("Topo {}: Removing all assignments of topology", topologyId);

                // the schedule is one number per line, each being the index of the partition the task for the given
                // line should be assigned to. The line number is the task index (0-based!).
                // the partition ids are 0-based because the METIS partitionings use that format
                numWorkers = 0;
                taskToWorkerIndexString = schedule.split("\n");
                taskToWorkerIndex = new int[taskToWorkerIndexString.length];
                for (int i = 0; i < taskToWorkerIndexString.length; i++) {
                    int worker = Integer.parseInt(taskToWorkerIndexString[i]); // worker = partition
                    taskToWorkerIndex[i] = worker;
                    numWorkers = Math.max(numWorkers, worker + 1); // partitions are 0-based
                }

                LOG.info("Topo {}: Schedule is considering {} tasks that have to be scheduled to {} workers",
                        new Object[] {topologyId, taskToWorkerIndex.length, numWorkers});

                // initialize the assignment collections
                tasksOfPartition = new ArrayList<>();
                for (int i = 0; i < numWorkers; i++) {
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
                    // if you get exception around here for taskIds not matching, maybe task ids are not always 1-based ;-)
                    taskId = executor.getStartTask();

                    if (taskId < 1) {
                        LOG.trace("Topo {}: Assigning special task with id {} of component {} to partition 0",
                                new Object[] {
                                        topologyId,
                                        taskId,
                                        topology.getExecutorToComponent().get(executor).toString()
                                });
                    } else if (taskId <= taskToWorkerIndex.length) {
                        int partitionId;

                        partitionId = taskToWorkerIndex[taskId-1];
                        LOG.trace("Topo {}: assigning task with id {} to partition {}",
                                new Object[] {topologyId, taskId, partitionId});
                        tasksOfPartition.get(partitionId).add(executor);
                    } else { // for all ackers and metrics, we do round-robbing
                        int partitionId;

                        partitionId = taskId % numWorkers;
                        LOG.trace("Topo {}: assigning task with id {} to partition {} (round-robin)",
                                new Object[]{topologyId, taskId, partitionId});
                        tasksOfPartition.get(partitionId).add(executor);
                    }
//                    else {
//                        StringBuilder taskIDs;
//                        String errorMsg;
//                        int numExecutorsInTopology;
//
//                        numExecutorsInTopology = topology.getExecutors().size();
//                        taskIDs = new StringBuilder();
//                        for (ExecutorDetails executorInner : topology.getExecutors()) {
//                            if (taskIDs.length() > 0) {
//                                taskIDs.append(", ");
//                            }
//                            taskIDs.append(executorInner.getStartTask());
//                        }
//
//                        errorMsg = String.format("Topo %s: Incompatible schedule found in zookeeper: There are only " +
//                                "assignments for %d tasks, while the topology has %d executors. Skipping " +
//                                "topology. The schedule found looks as follows:\n" +
//                                "%s\n" +
//                                "taskIDs for tasks to assign:\n" +
//                                "%s",
//                                topologyId, taskToWorkerIndex.length, numExecutorsInTopology,
//                                schedule.replaceAll("\n", ", "), taskIDs.toString());
//                        LOG.debug(errorMsg);
//
//                        unscheduledTopologies.put(topologyId, topology);
//                        continue with_next_topology; // continue with next topology, maybe it will work when we are called next time
//                    }

                }

                // free all currently used slots
                existingAssignment = cluster.getAssignmentById(topologyId);
                if (existingAssignment != null) {
                    LOG.debug("Topo {}: Freeing all currently used slots", topologyId);
                    for (WorkerSlot slot : existingAssignment.getSlots()) {
                        cluster.freeSlot(slot);
                    }
                }

                /*
                    The assignment process works as follows:
                    1.  create a linked list from all supervisors by putting all the supervisors that have currently no
                        workers assigned to the beginning of the list and partially busy supervisors at the end of the
                        list. Blacklisted hosts will be ignored.
                    2.  while there are still un-assigned partitions:
                    2.1 select next supervisor
                    2.2 get a list of all workers on the selected supervisor
                    2.3 distribute all tasks of that partition among the supervisors the given host
                 */
                availableSupervisors = new LinkedList<>();
                for (SupervisorDetails supervisor : cluster.getSupervisors().values()) {
                    if (!cluster.isBlackListed(supervisor.getId()) &&               // supervisor is not blacklisted ...
                            (cluster.getAssignableSlots(supervisor).size() > 0)) {  // ... and has free slots
                        if (cluster.getUsedPorts(supervisor).size() > 0) {
                            availableSupervisors.addLast(supervisor); // has used ports
                        } else {
                            availableSupervisors.addFirst(supervisor); // totally unoccupied supervisor
                        }
                    }
                }

                // check if there are enough supervisors available to fully schedule the topology
                if (availableSupervisors.size() < tasksOfPartition.size()) {
                    Log.warn("Could not schedule topology {} " +
                            " because there are no more supervisors available", topologyId);
                    continue with_next_topology;
                }

                // now assign the work
                LOG.debug("Applying the schedule to the storm topology {}", topologyId);
                for (Set<ExecutorDetails> tasks : tasksOfPartition) {
                    SupervisorDetails supervisor;
                    List<WorkerSlot> supervisorWorkers;
                    ArrayList<Set<ExecutorDetails>> tasksForWorker;
                    int numWorkersOfSupervisor;
                    int taskIdx; // used to distribute the tasks among the worker slots
                    int workerIdx;

                    numWorkersOfSupervisor = 0;
                    supervisor = null;
                    supervisorWorkers = null;

                    // we loop until we find a supervisor that has a worker ...
                    while (numWorkersOfSupervisor == 0) {
                        if (availableSupervisors.size() == 0) { // ... or until there are no more supervisors
                            throw new IllegalStateException("Could not schedule topology " + topologyId +
                                    " because there are no more supervisors available");
                        }

                        supervisor = availableSupervisors.removeFirst();
                        supervisorWorkers = cluster.getAssignableSlots(supervisor);

                        // distribute tasks among all worker slots
                        numWorkersOfSupervisor = supervisorWorkers.size();
                    }

                    LOG.trace("Topo {}: Assigning {} tasks to {} workers on supervisor {}",
                           new Object[] {topologyId, tasks.size(), numWorkersOfSupervisor, supervisor.getHost()});

                    tasksForWorker = new ArrayList<>();
                    for (int i = 0; i < numWorkersOfSupervisor; i++) {
                        tasksForWorker.add(new HashSet<ExecutorDetails>());
                    }
                    taskIdx = 0;
                    for (ExecutorDetails task : tasks) {
                        tasksForWorker.get(taskIdx % numWorkersOfSupervisor).add(task);
                        taskIdx++;
                    }

                    // assign the tasks to the workers
                    workerIdx = 0;
                    for (WorkerSlot worker : supervisorWorkers) {
                        if (cluster.isSlotOccupied(worker)) {
                            // todo: this is fine for research but will never work in a production setting as it
                            // could result in an endless loop of re-assignments if there are more workers
                            // requested than available.
                            cluster.freeSlot(worker);
                        }
                        cluster.assign(worker, topologyId, tasksForWorker.get(workerIdx));
                        workerIdx++;
                    }
                }

                // topology was successfully scheduled, remember the schedule we used for this
                this.scheduleMap.put(topologyId, schedule);
            } else {
                LOG.trace("The schedule found in Zookeeper has already been applied. Not doing anything.");
            }

        }

        if (unscheduledTopologies.size() > 0) {
            LOG.debug("Scheduling {} remaining topologies using the even scheduler ...", unscheduledTopologies.size());
            this.evenScheduler.schedule(new Topologies(unscheduledTopologies), cluster);
        }

    }

}
