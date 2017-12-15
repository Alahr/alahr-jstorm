package com.alahr.jstorm.example.cluster.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GenerateSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(GenerateSpout.class);

    private SpoutOutputCollector collector;

    private static String[] sentences = {
            "hello world",
            "hello storm",
            "this is the first example of storm",
            "welcome to the world of storm"
    };

    private static int index = 0;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector){
        this.collector = collector;
    }

    public void nextTuple(){
        if(index >= sentences.length){
            return;
        }
        logger.info("GenerateSpout:\t"+sentences[index]);
        this.collector.emit(new Values(sentences[index]));
        index++;
    }

    public void ack(Object id) {
    }

    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void close(){
    }
}
