package com.alahr.jstorm.kafka.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImplExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

public class KafkaBolt extends BaseBasicBolt {

    protected final Logger logger = LoggerFactory.getLogger(KafkaBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] bytes = (byte[]) ((TupleImplExt) input).get("bytes");
        try{
            String msg = new String(bytes, "utf-8");
            logger.info("-----------------------------------------");
            logger.info(msg);
        }
        catch (UnsupportedEncodingException e){
            logger.error("msg error", e);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println("declareOutputFields");
    }
}
