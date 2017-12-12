package com.alahr.jstorm.example.cluster.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CalNumBolt extends BaseRichBolt {
    private static final Logger logger = LoggerFactory.getLogger(CalNumBolt.class);

    private OutputCollector collector;

    private Map<String, Integer> counts;

    public CalNumBolt() {
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        logger.info("CalNumBolt\t"+word);
        Integer count = this.counts.get(word);
        if(count == null){
            count = 0;
        }
        count++;
        this.counts.put(word, count);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void cleanup() {
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        logger.info("--------Final result start--------");
        for(String key : keys){
            System.out.println(key + " "+this.counts.get(key));
        }
        logger.info("--------Final result end--------");
    }
}
