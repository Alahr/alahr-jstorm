package com.alahr.jstorm.example.wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;

    private String outputFile;

    private Map<String, Integer> counts;

    public WordCountBolt() {
    }

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        outputFile = conf.get("outputFile").toString();
        counts = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
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

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(outputFile);
            for(String key : keys){
                fileWriter.write(key + " "+this.counts.get(key)+"\n");
            }
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try{
                if(null != fileWriter) {
                    fileWriter.close();
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}
