Revision History
================
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