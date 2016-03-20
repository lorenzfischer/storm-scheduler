Storm-Scheduler
===============

This repository contains the code for the graph partitioning based storm scheduler presented in the paper

**Lorenz Fischer, Abraham Bernstein: *Workload scheduling in distributed stream processors using graph partitioning.* 2015 IEEE International Conference on Big Data, Big Data 2015, Santa Clara, CA, USA, October 29 - November 1, 2015. IEEE, 2015.**

The main parts of the code are:

1. The scheduler (https://github.com/lorenzfischer/storm-scheduler/tree/master/stools-core)
  
  * The java scheduler code: https://github.com/lorenzfischer/storm-scheduler/tree/master/stools-core/src/main/java/ch/uzh/ddis/stools/scheduler
  * The python script that reads the sendgraph from zookeeper, calls METIS and writes the resulting schedule back into zookeeper: https://github.com/lorenzfischer/storm-scheduler/blob/master/stools-core/src/dev/python/scheduler.py

2. The evaluated topologies
  * Parallel-Topo: https://github.com/lorenzfischer/storm-scheduler/blob/master/stools-topos/src/main/java/ch/uzh/ddis/stools/topos/ParallelTopology.java
  * Payload-Topo: https://github.com/lorenzfischer/storm-scheduler/blob/master/stools-topos/src/main/java/ch/uzh/ddis/stools/topos/PayloadTopology.java
  * FixedRate-Topo (to test impact of acking facility): https://github.com/lorenzfischer/storm-scheduler/blob/master/stools-topos/src/main/java/ch/uzh/ddis/stools/topos/FixedRateTopology.java

The following code is not part of this repo:

  * OpenGov-Topo: This topology can be found here: https://github.com/uzh/katts
  * Reference-Topo: http://www.dis.uniroma1.it/~midlab/software/ storm-adaptive-schedulers.zip
  