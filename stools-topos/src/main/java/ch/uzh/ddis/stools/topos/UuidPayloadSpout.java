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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class UuidPayloadSpout extends UuidSpout {

    private final int payloadFactor;

    private String payload;

    public UuidPayloadSpout(boolean disableAniello, int payloadFactor) {
        super(disableAniello);
        this.payloadFactor = payloadFactor;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyfield", "payload"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        this.payload = StringUtils.repeat(this.uuid, payloadFactor);
    }

    @Override
    public void nextTuple() {
        if (!this.disableAniello) {
            taskMonitor.checkThreadId();
        }

        this.emitCount++; // we start with msgId = 1
        this.collector.emit(new Values(this.uuid, this.payload), this.emitCount);
        if ((emitCount % (100 * 1000) ) == 0) {
            LOG.info("Emitted {} tuples", this.emitCount);
            Utils.sleep(1);
        }
    }
}
