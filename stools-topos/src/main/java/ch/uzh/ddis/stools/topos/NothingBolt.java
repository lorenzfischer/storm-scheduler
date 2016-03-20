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

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;

import java.util.Map;

/**
 * This bolt does (almost) nothing. For each tuple it receives, it takes the first element and emits it again. After
 * that, it acknowledges the tuple.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class NothingBolt extends BaseRichBolt {

    private OutputCollector collector;

    protected TaskMonitor taskMonitor;

    /**
     * If this is set to true, the statistics for Aniello's scheduler will not be collected.
     */
    protected boolean disableAniello;

    public NothingBolt(boolean disableAniello) {
        this.disableAniello = disableAniello;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // this object is used in the emit/execute method to compute the number of inter-node messages
        this.taskMonitor = new TaskMonitor(context.getThisTaskId());

        this.collector = collector;

        if (!this.disableAniello) {
            // this will create/configure the worker monitor once per worker
            WorkerMonitor.getInstance().setContextInfo(context);

            // this object is used in the emit/execute method to compute the number of inter-node messages
            this.taskMonitor = new TaskMonitor(context.getThisTaskId());
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!this.disableAniello) {
            taskMonitor.notifyTupleReceived(input);
        }

        this.collector.emit(input, new Values(input.getString(0))); // we assume there is only one field
        this.collector.ack(input);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyfield"));
    }
}
