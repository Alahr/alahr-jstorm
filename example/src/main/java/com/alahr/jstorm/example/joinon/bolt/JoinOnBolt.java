package com.alahr.jstorm.example.joinon.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class JoinOnBolt extends BaseRichBolt {

    private Logger logger = LoggerFactory.getLogger(JoinOnBolt.class);

    private OutputCollector collector;

    private ConcurrentHashMap<String, Integer> joinCountMap;

    public JoinOnBolt() {
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        joinCountMap = new ConcurrentHashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        String joinKey = tuple.getStringByField("joinKey");
        if("person".equals(streamId)){
            if(!joinCountMap.containsKey(joinKey)){
                joinCountMap.put(joinKey, 0);
            }
        }
        else if("animal".equals(streamId)){
            if(joinCountMap.containsKey(joinKey)){
                joinCountMap.put(joinKey, joinCountMap.get(joinKey)+1);
            }
        }
        else{

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("word"));
    }

    public void cleanup() {
        logger.info("-----------------start--------------------");
        Set<Map.Entry<String, Integer>> entries = joinCountMap.entrySet();
        for(Map.Entry<String, Integer> entry : entries){
            logger.info(entry.getKey()+":\t"+entry.getValue());
        }
        logger.info("-----------------end--------------------");
    }
}
