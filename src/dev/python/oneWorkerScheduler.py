__author__ = 'lfischer'

import sys
import time
import collections
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType
import logging

zk_connect_string   = "127.0.0.1:2000"
sendgraph_zk_path   = "/stools/scheduler/sendgraph"  # where we expect the sendgraphs to be written to
schedule_zk_path    = "/stools/scheduler/schedule"   # where we will write the schedules to
workerbeats_zk_path = "/storm/workerbeats/"          # where we go to find out how many workers per topo there are



def create_empty_sendgraph():
    """ This method creates a two dimentional default dictionary that can be used to reconstruct the sendgraph. """
    return collections.defaultdict(collections.defaultdict)


def byteArrayToInt(byteArray):
    """ This method converts the contents of the ZK nodes, which were long values in Java, into int values. """
    return int(byteArray.encode('hex'), 16)


def main_loop():
    logging.basicConfig()

    zk = KazooClient(hosts=zk_connect_string)
    zk.start()

    # zk.get_children("/storm/workerbeats")
    for topology in zk.get_children(sendgraph_zk_path):
        topology_sendgraph_path = sendgraph_zk_path + "/" + topology
        topology_schedule_path  = schedule_zk_path  + "/" + topology
        print("computing schedule for " + topology)

        # register a data watch for each
        def watchFunc(data, stat, event):
            #print("watch called")
            if event is not None and event.type == EventType.CHANGED:
                print("new sendgraph data for {0} at {1}".format(topology_sendgraph_path, byteArrayToInt(data)))

                sendgraph = create_empty_sendgraph()
                schedule = ""

                # extract the sendgraph from zookeeper
                for from_to in zk.get_children(topology_sendgraph_path):
                    # read the actual value from the from-to node
                    (value, node_stat) = zk.get("/".join((sendgraph_zk_path, topology, from_to)))

                    # the from_to string contains two ids separated by a single dash, so:
                    (from_task, to_task) = map(int, from_to.split("-"))  # convert strings to ints

                    sendgraph[from_task][to_task] = byteArrayToInt(value)

                # create the schedule (which in this case is just to schedule everything to one worker)

                #num_workers = len(zk.get_children(workerbeats_zk_path + "/" + topology))

                # we simulate the output of a metis schedule, which one line per vertex, each having one number
                # which is the partition (worker) it should be assigned to.
                for from_task in sendgraph.keys():
                    if len(schedule) > 0:
                        schedule += "\n"
                    schedule += "0"

                # write sendgraph to zk
                zk.ensure_path(topology_schedule_path)
                zk.set(topology_schedule_path, bytes(bytearray(schedule)))

            return True  # returning false will disable the watch

        # install data watch
        DataWatch(zk, topology_sendgraph_path, func=watchFunc)


    while 1:
        # wait until we get cancelled
        time.sleep(0.1)

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)
