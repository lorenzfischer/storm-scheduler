#!/usr/bin/python

import collections


partition_file = "tmp.graph.part.8"

assignments = collections.defaultdict(lambda: [])

with open(partition_file) as f:
    taskId = 1
    for line in f.readlines():
        assignments[line.strip()].append(str(taskId))
        taskId += 1

for key in sorted(assignments.keys()):
    print("{key}: {values}".format(key=key, values=",".join(assignments.get(key))))