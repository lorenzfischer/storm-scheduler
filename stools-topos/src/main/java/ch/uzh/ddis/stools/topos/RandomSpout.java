package ch.uzh.ddis.stools.topos;/* TODO: License */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rnd = new Random();
    }

    @Override
    public void nextTuple() {
        final String message = words[rnd.nextInt(words.length)];
        this.collector.emit(new Values(message));
    }
}
