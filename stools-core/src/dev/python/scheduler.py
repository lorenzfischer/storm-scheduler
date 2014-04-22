__author__ = 'lfischer'

import sys
import time
import collections
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType
import logging


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

def create_empty_sendgraph():
    """ This method creates a two dimentional default dictionary that can be used to reconstruct the sendgraph. """
    return collections.defaultdict(collections.defaultdict)



def byteArrayToInt(byteArray):
    """ This method converts the contents of the ZK nodes, which were long values in Java, into int values. """
    return int(byteArray.encode('hex'), 16)



def scheduleOneWorker(num_workers, jsonSendgraph):
    """This scheduler assigns all work to only one scheduler."""
    all_tasks = set()
    schedule = ""

    # get a list of all tasks
    for from_task in sendgraph.keys():
        for to_task in sendgraph[from_task].keys():
            all_tasks.add(from_task)
            all_tasks.add(to_task)

    print("assigning all tasks to one partition.")
    # we simulate the output of a metis schedule, which one line per vertex, each having one number
    # which is the partition (worker) it should be assigned to.
    for task in all_tasks:
        if len(schedule) > 0:
            schedule += "\n"
        schedule += "0"

    return schedule



def createMetisGraphFile(sendgraph_original):
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
    def one():
        return 1
    graph_file = "tmp.graph"
    vertex_weights = collections.defaultdict(one)  # we put a minimum weight of one as zero is not supported by metis

    # we need to copy the whole send graph and make sure that the graph is bi-directional (metis supports only
    # bi-directional graphs. We do this by inserting backward-edges of weight 1. At the same time we count the
    # number of vertices and edges, both necessary for metis to work.
    sendgraph = create_empty_sendgraph()

    # copy send graph and insert backward edges +
    for from_task in sendgraph_original.keys():
        for to_task in sendgraph_original[from_task].keys():
            if to_task != from_task:
                sendgraph[from_task][to_task] = sendgraph_original[from_task][to_task]
                sendgraph[to_task][from_task] = 1  # the backward-edge

    # create map with vertex weights + count number of edges
    edge_count = 0
    for from_task in sendgraph.keys():
        for to_task in sendgraph[from_task].keys():
            if to_task != from_task:
                # we estimate the vertex weights by the number of incoming messages
                vertex_weights[to_task] += sendgraph[from_task][to_task]
                edge_count += 1

    edge_count /= 2  # we only count the forward-edges as metis doesn't support directed graphs

    graph_file_content = "{n} {m} 011 1".format(n=len(vertex_weights.keys()), m=edge_count)

    for from_vertex in range(1, len(vertex_weights) + 1):  # indexed from 1..num_Vertex
        vertex_line = ""
        for to_vertex in sendgraph[from_vertex].keys():
            if len(vertex_line) > 0:
                vertex_line += " "
            vertex_line += "{v} {w}".format(v=to_vertex, w=sendgraph[from_vertex][to_vertex])
        # prepend vertex weight
        vertex_line = "{vw} ".format(vw=vertex_weights[from_vertex]) + vertex_line

        graph_file_content = graph_file_content + "\n" + vertex_line

    with open(graph_file, 'w') as f:
        f.write(graph_file_content)

    return graph_file


def schedulerMetis(num_workers, jsonSendgraph):
    """This scheduler uses METIS to partition the graph and schedule the work."""
    from subprocess import check_output, CalledProcessError

    schedule = ""

    # Create METIS schedule
    print("Creating METIS graph file")
    graph_file = createMetisGraphFile(jsonSendgraph)
    partition_file = "{gf}.part.{partitions}".format(gf=graph_file, partitions=num_workers)

    # todo: objtype=cut is not necessary I think
    print("Calling METIS")
    try:
        metis_output = check_output([metis_binary, "-objtype=cut", graph_file, str(num_workers)])

        # read the partition file
        with open(partition_file) as f:
            schedule = ""
            for line in f.readlines():
                if len(line) > 0:
                    schedule += line
    except CalledProcessError as e:
        print("metis threw an exception:\n" + e.output)

    # print(schedule)
    # exit(1)
    return schedule



# here we configure the scheduler currently in use (either schedulerOneWorker or schedulerMetis)
scheduler = schedulerMetis



def schedule(zk, topology):
    topology_metrics_path = metrics_zk_path + "/" + topology
    topology_sendgraph_path = topology_metrics_path + "/sendgraph"
    topology_config_path = topology_metrics_path + "/config"
    topology_schedule_path = schedule_zk_path + "/" + topology

    print("scheduling using data at {0} writing schedule into {1}".
          format(topology_metrics_path, topology_schedule_path))

    sendgraph = create_empty_sendgraph()
    schedule = ""

    # get number of workers for topology
    (nw_value, nw_stat) = zk.get(topology_config_path + "/topology.workers")
    num_workers = byteArrayToInt(nw_value)

    # extract the sendgraph from zookeeper
    zk.get()

    for from_to in zk.get_children(topology_sendgraph_path):
        # read the actual value from the from-to node
        (value, node_stat) = zk.get(topology_sendgraph_path + "/" + from_to)

        # the from_to string contains two ids separated by a single dash, so:
        (from_task, to_task) = map(int, from_to.split("_"))
        sendgraph[from_task][to_task] = byteArrayToInt(value)

    # the output of the schedules need to be the same as the partitioning files of METIS:
    # one line per vertex, each having one number which is the partition (worker) it should
    # be assigned to.
    schedule = scheduler(num_workers, sendgraph)

    if len(schedule) > 0:
        #print("computed schedule:\n{1}".format(len(sendgraph.keys()), schedule))

        # write schedule to zk
        print("writing schedule to zookeeper")
        zk.ensure_path(topology_schedule_path)
        zk.set(topology_schedule_path, bytes(bytearray(schedule)))
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
        DataWatch(zk, topology_metrics_zk_path, func=watchFunc)

        # if there is some data already, schedule immediately
        if len(zk.get_children(topology_metrics_zk_path)):
            print("existing sendgraph data for {0}".format(topology))
            schedule(zk, topology)


    while 1:
        # wait until we get cancelled
        time.sleep(0.1)

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)
