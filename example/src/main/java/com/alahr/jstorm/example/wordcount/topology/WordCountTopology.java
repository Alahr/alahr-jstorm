package com.alahr.jstorm.example.wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.alahr.jstorm.example.wordcount.bolt.SplitSentenceBolt;
import com.alahr.jstorm.example.wordcount.bolt.WordCountBolt;
import com.alahr.jstorm.example.wordcount.spout.SentenceSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountTopology {
    private static Logger logger = LoggerFactory.getLogger(WordCountTopology.class);

    public static void main(String[] args){

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentence-spout", new SentenceSpout(), 1);
        builder.setBolt("split-bolt", new SplitSentenceBolt(), 2).localOrShuffleGrouping("sentence-spout");
        builder.setBolt("count-bolt", new WordCountBolt()).fieldsGrouping("split-bolt", new Fields("word"));


        String inputFile = "file/input.txt";
        String outputFile = "file/output.txt";

        Config conf = new Config();

        conf.put("inputFile", inputFile);
        conf.put("outputFile", outputFile);
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            logger.info("Cluster topology: {}", args[0]);
            try{
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (InvalidTopologyException e){
                e.printStackTrace();
            }
            catch (AlreadyAliveException e){
                e.printStackTrace();
            }
        }
        else {
            String topName = "example-word-count-topology";
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topName, conf, builder.createTopology());

            Utils.sleep(1000);
//            cluster.killTopology(topName);
            cluster.shutdown();
        }
    }
}
