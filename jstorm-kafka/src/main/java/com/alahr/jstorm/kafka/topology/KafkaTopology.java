package com.alahr.jstorm.kafka.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.alahr.jstorm.kafka.bolt.KafkaBolt;
import com.alibaba.jstorm.kafka.KafkaSpout;
import com.alibaba.jstorm.kafka.KafkaSpoutConfig;
import com.alahr.jstorm.common.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaTopology {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTopology.class);

    public static void main(String[] args){
        PropertiesUtil propertiesUtil = new PropertiesUtil("/kafka.properties", false);
        Map propsMap = propertiesUtil.getAllProperties();
        KafkaSpoutConfig spoutConfig = new KafkaSpoutConfig(propertiesUtil.getProperties());
        spoutConfig.configure(propsMap);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
        builder.setBolt("kafkaBolt", new KafkaBolt()).localOrShuffleGrouping("kafkaSpout");
        //Configuration
        Config conf = new Config();
        conf.setMaxSpoutPending(1000);
        conf.setMessageTimeoutSecs(1000);
        conf.setDebug(true);
        //指定使用logback.xml
        //需要把logback.xml文件放到jstorm conf目录下
//        ConfigExtension.setUserDefinedLogbackConf(conf, "%JSTORM_HOME%/conf/logback.xml");
        if (args != null && args.length > 0) {
            //提交到集群运行
            try{
                StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            }
            catch (InvalidTopologyException e){
                logger.error("invalid topology", e);
            }
            catch (AlreadyAliveException e){
                logger.error("already alive", e);
            }
        } else {
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaTopology", conf, builder.createTopology());
        }
    }
}
