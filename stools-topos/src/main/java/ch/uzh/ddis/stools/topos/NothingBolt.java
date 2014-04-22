package ch.uzh.ddis.stools.topos;/* TODO: License */

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * This bolt does (almost) nothing. For each tuple it receives, it takes the first element and emits it again. After
 * that, it acknowledges the tuple.
 *
 * @author "Lorenz Fischer" <lfischer@ifi.uzh.ch>
 */
public class NothingBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        this.collector.emit(input, new Values(input.getString(0))); // we assume there is only one field
        this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}
