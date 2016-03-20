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
import midlab.storm.scheduler.TaskMonitor;
import midlab.storm.scheduler.WorkerMonitor;

import java.util.Map;
import java.util.Random;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class RandomSpout extends BaseRichSpout {

    private String[] words = ("is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been " +
            "the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type" +
            " and scrambled it to make a type specimen book. It has survived not only five centuries, but also the " +
            "leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with " +
            "the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing" +
            " software like Aldus PageMaker including versions of Lorem Ipsum.").split(" ");

    private Random rnd;

    private SpoutOutputCollector collector;

    protected TaskMonitor taskMonitor;

    /**
     * If this is set to true, the statistics for Aniello's scheduler will not be collected.
     */
    protected boolean disableAniello;

    public RandomSpout(boolean disableAniello) {
        this.disableAniello = disableAniello;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyfield"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rnd = new Random();

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

        final String message = words[rnd.nextInt(words.length)];
        this.collector.emit(new Values(message));
    }
}
