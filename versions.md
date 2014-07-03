Revision History
================
1.7.2-SNAPSHOT - skip to next topology if there are no more supervisors to assign a topology
1.7.1-SNAPSHOT - fix a bug in the ZookeeperScheduler in which the scheduler would fail, when it tried
                 to schedule work on a supervisor whose worker slots where all blocked.
1.7.0-SNAPSHOT - give each topology its own name and don't just just "test-topo" for all of them
1.6.3-SNAPSHOT - store numWorkers metric as long in zk, but read in as Number.
1.6.2-SNAPSHOT - store numWorkers metric as integer in zk not as long
1.6.1-SNAPSHOT - start each line of the metrics log with a tab and decrease padding of metrics names to 33
1.6.0-SNAPSHOT - cpu und network statistics collection
1.4.0-SNAPSHOT - count all emitted messages again
1.3.0-SNAPSHOT - new trident topo
1.1.1-SNPASHOT - try not measuring ackers and metrics again.

1.1.0-SNPASHOT - reduce number of ackers to 1, but measure traffic of ackers and include them in the scheduling

1.0.2-SNAPSHOT - assign non-scheduled nodes in a round-robin fashion (to accommodate for all the ackers and metrics)
1.0.1-SNPASHOT - remove the self-references from json, rename the "graph" node to "links"

0.9.1-SNAPSHOT - removed error message when tasks are found that don't exist in the schedule
0.9.0-SNAPSHOT - exclude acks and metrics, but use even scheduler to schedule nodes that are not contained in the
                 partitioning.

0.8.0-SNAPSHOT - include acks and metrics again

0.7.1-SNAPSHOT - corrected debug output + write 1 value for metric and ack channels
0.7.0-SNAPSHOT - don't measure "ack"-messages for creating the sendgraph.

0.6.1-SNAPSHOT - write json into zookeeper and only do it once every 15 seconds

0.5.1-SNAPSHOT - don't reset counters in each cycle. we will have to change this back in order to make it dynamic

0.4.2-SNAPSHOT - NullCheck
0.4.1-SNAPSHOT - NPE
0.4.0-SNAPSHOT - implement graphite metrics writer

0.3.2-SNAPSHOT - only free assignments when they actually exist
0.3.1-SNAPSHOT - store data under the topology name, rather than the topology-id (is the same across runs)

0.2.1-SNAPSHOT - don't break if znode exists already
0.2.0-SNAPSHOT - write number of workers into zk as well
0.1.4-SNAPSHOT - cosmetic change for log output
0.1.3-SNAPSHOT - added topologyId to log output
0.1.2-SNAPSHOT - null check for schedule content
0.1.1-SNAPSHOT - fixed bug "nothing was getting assigned to nothing"
0.1.0-SNAPSHOT - support for negative taskIDs (separate tasks with an underscore rather than a dash in zookeeper)
0.0.7-SNAPSHOT - don't include storm jar
0.0.6-SNAPSHOT - don't create zk-root-folder if it already exists
0.0.5-SNAPSHOT - free slots before reassigning them
0.0.4-SNAPSHOT - put 0-weight for each node
0.0.3-SNAPSHOT - output the taskIDs of all tasks that are to be scheduled
0.0.2-SNAPSHOT - don't delete anything, only update
0.0.1-SNAPSHOT - initial version