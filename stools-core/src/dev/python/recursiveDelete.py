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
from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from kazoo.protocol.states import EventType

def byteArrayToInt(byteArray):
    return int(byteArray.encode('hex'), 16)


sendgraph_zk_path = "/stools/scheduler/sendgraph"
zk_connect_string = "127.0.0.1:2000"

def createMetisGraph(sendgraph):
    """This method creates the contents that a graph input file needs to have in order to be processable by METIS
    (http://glaros.dtc.umn.edu/gkhome/projects/gp/products?q=views/metis). A METIS input file consists of one header
    line and of n lines, where n is the number of vertices in the graph. For our purposes the header line looks as
    follows:
        n m 011 1
    n and m are the number of vertices and edges in the graph respectively while the latter two elements tell METIS
    that we will have one weight on each vertex and that the edges are also weighted.

    All remaining lines are of the form

    more information about this format can be found here: http://glaros.dtc.umn.edu/gkhome/fetch/sw/metis/manual.pdf
    """

def main_loop():
    zk = KazooClient(hosts=zk_connect_string)
    zk.start()

    # zk.get_children("/storm/workerbeats")
    for topology in zk.get_children(sendgraph_zk_path):
        topology_path = sendgraph_zk_path + "/" + topology
        print("computing schedule for " + topology)

        # register a data watch for each
        def watchFunc(data, stat, event):
            #print("watch called")
            if event is not None and event.type == EventType.CHANGED:
                print("update at {0}".format(byteArrayToInt(data)))
                for from_to in zk.get_children(topology_path):
                    (value, node_stat) = zk.get("/".join((sendgraph_zk_path, topology, from_to)))
                    ival = byteArrayToInt(value)
                    print(from_to + ": " + str(ival))
            return True # returning false will disable the watch

        # install data watch

        DataWatch(zk, topology_path, func=watchFunc)


    while 1:
        # wait until we get cancelled
        time.sleep(0.1)

if __name__ == '__main__':
    try:
        main_loop()
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)
