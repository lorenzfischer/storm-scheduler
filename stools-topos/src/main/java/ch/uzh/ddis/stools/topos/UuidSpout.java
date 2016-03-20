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

package ch.uzh.ddis.stools.topos;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

/**
 * This spout computes a unique identifier (UUID) value and then keeps emitting this id forever.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class UuidSpout extends BaseRichSpout {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String uuid;

    protected SpoutOutputCollector collector;

    protected long emitCount;

    protected int numSpouts;

    protected int thisTaskIndex;

    protected TaskMonitor taskMonitor;

    /**
     * If this is set to true, the statistics for Aniello's scheduler will not be collected.
     */
    protected boolean disableAniello;

    public UuidSpout(boolean disableAniello) {
        this.disableAniello = disableAniello;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyfield"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        MessageDigest md;
        int counter;

        this.thisTaskIndex = context.getThisTaskIndex();
        this.numSpouts = context.getComponentTasks(context.getThisComponentId()).size();
        counter = 0;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Couldn't find MD5 algorithm.", e);
        }

        // we want to create a message that hashes to exacly one of the following spouts. As there are the same number
        // of bolts on each level as there are spouts, we just keep looking until we find a uuid whose hash code would
        // be assigned to the id of this spout (if it were a bolt).
        do {
            if (++counter > 1000 * 1000) {
                throw new RuntimeException("Unable to generate required UUID in 1 mio tries.");
            }
            byte[] bytes = md.digest(UUID.randomUUID().toString().getBytes());
            this.uuid = new String(bytes);
        } while (this.uuid.hashCode() % this.numSpouts != this.thisTaskIndex);

        this.collector = collector;

        if (!this.disableAniello) {
            // this will create/configure the worker monitor once per worker
            WorkerMonitor.getInstance().setContextInfo(context);

            // this object is used in the emit/execute method to compute the number of inter-node messages
            this.taskMonitor = new TaskMonitor(context.getThisTaskId());
        }
    }

    @Override
    public void nextTuple() {
        if (!this.disableAniello) {
            taskMonitor.checkThreadId();
        }

        this.emitCount++; // we start with msgId = 1
        this.collector.emit(new Values(this.uuid), this.emitCount);
        if ((emitCount % (100 * 1000)) == 0) {
            LOG.info("Emitted {} tuples", this.emitCount);
            Utils.sleep(1);
        }
    }

    @Override
    public void fail(Object msgId) {
        LOG.error("Message with Id {} failed.", msgId);
    }
}
