package com.alahr.jstorm.example.cluster.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.alahr.jstorm.example.cluster.bolt.CalNumBolt;
import com.alahr.jstorm.example.cluster.bolt.SubSentence;
import com.alahr.jstorm.example.cluster.spout.GenerateSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterTopology {
    private static final Logger logger = LoggerFactory.getLogger(ClusterTopology.class);

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("generate-spout", new GenerateSpout());
        builder.setBolt("sub-bolt", new SubSentence()).localOrShuffleGrouping("generate-spout");
        builder.setBolt("cal-bolt", new CalNumBolt()).fieldsGrouping("sub-bolt", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            try{
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            }
            catch (InvalidTopologyException e){
                logger.error("",e);
            }
            catch (AlreadyAliveException e){
                logger.error("",e);
            }
        }
        else {
            String topName = "example-cluster-topology";

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topName, conf, builder.createTopology());

            Utils.sleep(10000);
//            cluster.killTopology(topName);
            cluster.shutdown();
        }
    }
}
