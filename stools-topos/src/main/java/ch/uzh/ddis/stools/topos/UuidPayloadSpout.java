/* TODO: License */
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
        this.emitCount++; // we start with msgId = 1
        this.collector.emit(new Values(this.uuid, this.payload), this.emitCount);
        if ((emitCount % (100 * 1000) ) == 0) {
            LOG.info("Emitted {} tuples", this.emitCount);
            Utils.sleep(1);
        }
    }
}
