package ch.uzh.ddis.stools.topos;/* TODO: License */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.MD5;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * This spout computes a unique identifier (UUID) value and then keeps emitting this id forever.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class UuidSpout extends BaseRichSpout {

    private final static Logger LOG = LoggerFactory.getLogger(UuidSpout.class);

    private String uuid;

    private SpoutOutputCollector collector;

    private long emitCount;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        MessageDigest md;
        int numSpouts;
        int thisTaskIndex;
        int counter;

        numSpouts = context.getComponentTasks(context.getThisComponentId()).size();
        thisTaskIndex = context.getThisTaskIndex();
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

            if (++counter > 1000*1000) {
                throw new RuntimeException("Unable to generate required UUID in 1 mio tries.");
            }

            this.uuid = new String(md.digest(UUID.randomUUID().toString().getBytes()));
        } while (this.uuid.hashCode() %  numSpouts != thisTaskIndex);

        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(this.uuid));
        this.emitCount++;
        if ((emitCount % (100 * 1000) ) == 0) {
            LOG.info("Emitted {} tuples", this.emitCount);
        }
        Utils.sleep(1);
    }
}
