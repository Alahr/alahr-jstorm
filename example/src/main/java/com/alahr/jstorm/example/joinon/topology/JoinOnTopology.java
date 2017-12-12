package com.alahr.jstorm.example.joinon.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.alahr.jstorm.example.joinon.bolt.JoinOnBolt;
import com.alahr.jstorm.example.joinon.spout.AnimalSpout;
import com.alahr.jstorm.example.joinon.spout.PersonSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JoinOnTopology {
    private final static Logger logger = LoggerFactory.getLogger(JoinOnTopology.class);

    public static void main(String[] args){
        /**
         * person、animal两个表关联
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("person-spout", new PersonSpout());
        builder.setSpout("animal-spout", new AnimalSpout());
        builder.setBolt("join-on-bolt", new JoinOnBolt(), 2)
                .fieldsGrouping("person-spout", "person", new Fields("joinKey"))
                .fieldsGrouping("animal-spout", "animal", new Fields("joinKey"));

        Config conf = new Config();
        //建议加上这行，使得每个bolt/spout的并发度都为1
//        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);

        conf.setDebug(true);
        conf.setNumWorkers(4);
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
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("join-on-topology", conf, builder.createTopology());

            Utils.sleep(5000);
//            cluster.killTopology(topName);
            cluster.shutdown();
        }
    }

}
