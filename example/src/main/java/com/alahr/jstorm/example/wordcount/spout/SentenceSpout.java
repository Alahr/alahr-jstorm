package com.alahr.jstorm.example.wordcount.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout extends BaseRichSpout {
    private static Logger logger = LoggerFactory.getLogger(SentenceSpout.class);

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private BufferedReader bufferedReader;
    private boolean completed = false;

    private ConcurrentHashMap<String, Values> pending;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<String, Values>();
        try {
            String inputFile = conf.get("inputFile").toString();
            fileReader = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        if (completed) {
            return;
        }

        bufferedReader = new BufferedReader(fileReader);
        try {
            String sentence;
            int i = 1;
            while ((sentence = bufferedReader.readLine()) != null) {
                String messageId = "msg-" + i;
                System.out.println("sentence:\t" + sentence);
                Values vs = new Values(sentence);
                this.pending.put(messageId, vs);
                this.collector.emit(vs, messageId);
                i++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            completed = true;
        }
    }

    public void ack(Object msgId) {
        logger.info("ack messageId:\t" + msgId.toString());
        this.pending.remove(msgId);
    }

    public void fail(Object msgId) {
        logger.error("fail messageId:\t" + msgId.toString() + ", then emit again");
        this.collector.emit(this.pending.get(msgId), msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void close() {
        try {
            if (null != this.fileReader) {
                this.fileReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
