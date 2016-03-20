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
 * @see ch.uzh.ddis.stools.topos.UuidFixedRatePayloadSpout#UuidFixedRatePayloadSpout(boolean, int, int)
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class UuidFixedRatePayloadSpout extends UuidPayloadSpout {

    /** This defines the number of tuples that can be emitted each second. */
    private final int tuplesPerSecond;

    /** This variable is used to determine when the next second has started. */
    private long lastSecond = 0L;

    /** This variable is used to throttle the speed at which tuples get emitted. At the beginning of each new
     * second, this value is set to {@link #tuplesPerSecond}. When this value reaches zero, no more tuples are emitted
     * until the "next second" starts. */
    private int tuplesRemainingThisSecond = 0;

    /**
     * This spout is designed to be used in a topology where the acking facility has been turned off. It emits tuples
     * at a fixed rate per second which is configured in the tuplesPerSecond parameter. Each second, the spout emits
     * tuples at full speed until the configured number of tuples that can be emitted per second is reached. This means
     * that there is no guarantee for an evenly distributed number of tuples, but that there will be spikes at the
     * beginning of each second.
     *
     * @param disableAniello set to true to disable aniello's task monitoring code. We normally leave this on to
     *                       guarantee the same performance penalties during all runs.
     * @param payloadFactor  the number of times the id should be repeated to generate the payload for the tuples.
     * @param tuplesPerSecond the rate at which the spout should emit tuples.
     */
    public UuidFixedRatePayloadSpout(boolean disableAniello, int payloadFactor, int tuplesPerSecond) {
        super(disableAniello, payloadFactor);
        this.tuplesPerSecond = tuplesPerSecond;
    }

    @Override
    public void nextTuple() {
        long thisSecond;

        // find out if a new second has started and if so
        thisSecond = System.currentTimeMillis() / 1000;
        if (thisSecond != this.lastSecond) {
            // a new second has started
            this.lastSecond = thisSecond;
            this.tuplesRemainingThisSecond = this.tuplesPerSecond;
        }

        this.tuplesRemainingThisSecond--;             // bookkeeping
        if (this.tuplesRemainingThisSecond > 0) {     // emit if possible
            super.nextTuple();
        } else {                                      // throttle if necessary
            /** we should wait 1ms. {@see backtype.storm.spout.ISpout#nextTuple()} */
            Utils.sleep(1);
        }
    }
}
