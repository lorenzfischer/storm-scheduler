__author__ = 'lfischer'

# @author Lorenz Fischer
#
# Copyright 2016 University of Zurich
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time
import collections
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType
import logging
import json


#
# This script reads the sendgraph of a storm topology from zookeeper, creates a
# schedule based on the sendgraph and write this schedule back into zookeeper.
#
# At the moment there are two methos supported:
#  - schedulerOneWorker -> schedules all work to one workder
#  - schedulerMetis -> use METIS to schedule the work
#


#zk_connect_string   = "127.0.0.1:2000"
#zk_connect_string   = "claudio01.ifi.uzh.ch:2181"
zk_connect_string = "tentacle.ifi.uzh.ch:2181"
metrics_zk_path = "/stools/scheduler/metrics"      # where we expect the metrics to be written to
schedule_zk_path  = "/stools/scheduler/schedule"   # where we will write the schedules to
#workerbeats_zk_path = "/storm/workerbeats/"       # where we go to find out how many workers per topo there are
metis_binary = "/Users/lfischer/tools/metis-5.1.0/build/Darwin-x86_64/programs/gpmetis"

# def create_empty_sendgraph():
#     """ This method creates a two dimentional default dictionary that can be used to reconstruct the sendgraph. """
#     return collections.defaultdict(collections.defaultdict)



def byteArrayToInt(byteArray):
    """ This method converts the contents of the ZK nodes, which were long values in Java, into int values. """
    return int(byteArray.encode('hex'), 16)



def scheduleOneWorker(num_workers, jsonSendGraph):
    """This scheduler assigns all work to only one scheduler."""
    all_tasks = set()
    schedule = ""

    # get a list of all tasks
    for node in jsonSendGraph['nodes']:
        all_tasks.add(int(node['name']))

    print("assigning all tasks to one partition.")
    # we simulate the output of a metis schedule, which one line per vertex, each having one number
    # which is the partition (worker) it should be assigned to.
    for task in all_tasks:
        if len(schedule) > 0:
            schedule += "\n"
        schedule += "0"

    return schedule



def createMetisGraphFile(jsonSendgraph):
    """This method creates the contents that a graph input file needs to have in order to be processable by METIS
    (http://glaros.dtc.umn.edu/gkhome/projects/gp/products?q=views/metis). A METIS input file consists of one header
    line and of n lines, where n is the number of vertices in the graph. For our purposes the header line looks as
    follows:
        n m 011 1
    n and m are the number of vertices and edges in the graph respectively while the latter two elements tell METIS
    that we will have one weight on each vertex and that the edges are also weighted.

    All remaining lines are of the form

        vw v1 w1 ... w1 wN

    where the ith line contains information about the ith vertex. On each line vw is the weight of the vertex,
    v1..vN are the vertices adjacent to the ith vertex and w1..wN are the weights for these vertices.

    more information about this format can be found here: http://glaros.dtc.umn.edu/gkhome/fetch/sw/metis/manual.pdf
    (page 10 at the very top).
    """
    graph_file = "tmp.graph"
    #  default is a dictionary of a default dictionary with a default weight of 0
    edge_weights = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))  # default value is 0
    vertex_weights = collections.defaultdict(lambda: 1)  # default weight of 1 -  zero is not supported by metis
    max_node_id = 0


    # build the connection matrix
    edge_count = 0
    for link in jsonSendgraph['links']:
        sourceId = int(link['source'])
        targetId = int(link['target'])
        value = int(link['value'])

        if sourceId > max_node_id:
            max_node_id = sourceId
        if targetId > max_node_id:
            max_node_id = targetId

        if sourceId != targetId:
            if edge_weights[sourceId][targetId] == 0:  # only count the edge if it does not yet exist
                edge_count += 1
            edge_weights[sourceId][targetId] += value
            # metis requires a reverse link, so we put a value of 1 if it does not yet exist
            if edge_weights[targetId][sourceId] == 0:
                edge_weights[targetId][sourceId] = 1
            vertex_weights[targetId] += value
            vertex_weights[sourceId] += value  # count outgoing messages as well to vertex weight

    # count nodes and links
    vertex_count = max_node_id

    graph_file_content = "{n} {m} 011 1".format(n=vertex_count, m=edge_count)

    for from_vertex in range(1, vertex_count + 1):  # indexed from 1..num_Vertex
        vertex_line = ""
        for to_vertex in edge_weights[from_vertex].keys():
            if len(vertex_line) > 0:
                vertex_line += " "
            vertex_line += "{v} {w}".format(v=to_vertex, w=edge_weights[from_vertex][to_vertex])
        # prepend vertex weight
        vertex_line = "{vw} ".format(vw=vertex_weights[from_vertex]) + vertex_line

        graph_file_content = graph_file_content + "\n" + vertex_line

    with open(graph_file, 'w') as f:
        f.write(graph_file_content)

    # print("Metis file content:\n{0}".format(graph_file_content))

    return graph_file


def schedulerMetis(num_workers, jsonSendgraph):
    """This scheduler uses METIS to partition the graph and schedule the work."""
    import subprocess

    schedule = ""

    # Create METIS schedule
    print("Creating METIS graph file for {0} partitions".format(num_workers))
    graph_file = createMetisGraphFile(jsonSendgraph)
    partition_file = "{gf}.part.{partitions}".format(gf=graph_file, partitions=num_workers)

    # todo: objtype=cut is not necessary I think
    print("Calling METIS")
    try:
        metis_output = subprocess.check_output([metis_binary, "-objtype=vol", graph_file, str(num_workers)])
        #print("metis output:\n{0}".format(metis_output))

        # read the partition file
        with open(partition_file) as f:
            schedule = ""
            for line in f.readlines():
                if len(line) > 0:
                    schedule += line
    except subprocess.CalledProcessError as e:
        print("something went wrong when calling metis:\n" + e.output)

    # print(schedule)
    # exit(1)
    return schedule



# here we configure the scheduler currently in use (either schedulerOneWorker or schedulerMetis)
scheduler = schedulerMetis
# scheduler = scheduleOneWorker



def schedule(zk, topology):
    topology_metrics_path = metrics_zk_path + "/" + topology
    topology_sendgraph_path = topology_metrics_path + "/sendgraph"
    topology_config_path = topology_metrics_path + "/config"
    topology_schedule_path = schedule_zk_path + "/" + topology

    print("scheduling using data at {0} writing schedule into {1}".
          format(topology_metrics_path, topology_schedule_path))

    jsonStr = ""
    retry = 0
    while len(jsonStr) == 0:
        retry += 1
        if retry >= 24:  # try for two minutes
            raise RuntimeError("Could not find send graph data in Zookeeper")
        elif retry > 1:
            print("Could not find send graph data in Zookeeper. Retrying in 10 seconds...")
            time.sleep(10)  # wait for 5 seconds before retrying

        # extract the sendgraph from zookeeper
        (jsonStr, node_stat) = zk.get(topology_sendgraph_path)

    # parse the json graph
    jsonSendGraph = json.loads(jsonStr)

    # write json to file for debugging
    with open("tmp.graph.json", 'w') as f:
        f.write(jsonStr)

     # get number of workers for topology
    (nw_value, nw_stat) = zk.get(topology_config_path + "/topology.workers")
    num_workers = byteArrayToInt(nw_value)

    # the output of the schedules need to be the same as the partitioning files of METIS:
    # one line per vertex, each having one number which is the partition (worker) it should
    # be assigned to.
    schedule = scheduler(num_workers, jsonSendGraph)
    #schedule = "" # turn the scheduler off

    if len(schedule) > 0:
        debug = False

        if debug:
            print("computed schedule for topo {0}:\n{1}".format(topology, schedule))
        else:
            # write schedule to zk
            print("writing schedule to zookeeper")
            zk.ensure_path(topology_schedule_path)
            zk.set(topology_schedule_path, bytes(bytearray(schedule)))
            print("schedule written")
    else:
        print("No schedule computed")



def main_loop():
    logging.basicConfig()

    zk = KazooClient(hosts=zk_connect_string)
    zk.start()

    # make sure the root folders for the sendgraph and the schedules exist
    zk.ensure_path(metrics_zk_path)
    zk.ensure_path(schedule_zk_path)

    for topology in zk.get_children(metrics_zk_path):
        topology_metrics_zk_path = metrics_zk_path + "/" + topology
        print("registering watcher schedule for " + topology_metrics_zk_path)

        # register a data watch for each
        def watchFunc(data, stat, event):
            #print("watch called")
            if event is not None and event.type == EventType.CHANGED:
                print("new sendgraph data for {0} at {1}".format(topology, byteArrayToInt(data)))
                schedule(zk, topology)
            return True  # returning false will disable the watch

        # install data watch
        #DataWatch(zk, topology_metrics_zk_path, func=watchFunc)

        # if there is some data already, schedule immediately
        if len(zk.get_children(topology_metrics_zk_path)):
            print("existing sendgraph data for {0}".format(topology))
            schedule(zk, topology)


    # while 1:
    #     # wait until we get cancelled
    #     time.sleep(0.1)

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)
