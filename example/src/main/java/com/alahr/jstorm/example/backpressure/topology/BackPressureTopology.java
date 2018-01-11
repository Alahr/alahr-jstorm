package com.alahr.jstorm.example.backpressure.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.alahr.jstorm.example.backpressure.bolt.BackPressureBolt;
import com.alahr.jstorm.example.backpressure.spout.BackPressureSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackPressureTopology {
    private static final Logger logger = LoggerFactory.getLogger(BackPressureTopology.class);

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new BackPressureSpout());
        builder.setBolt("bolt", new BackPressureBolt(), 10).localOrShuffleGrouping("spout");

        Config config = new Config();

        if (null != args && args.length > 0) {
            if("false".equals(args[1])){
                config.put("topology.backpressure.enable", false);
            } else {
                /*
                    ## 反压总开关
                    topology.backpressure.enable: true
                    ## 高水位 －－ 当队列使用量超过这个值时，认为阻塞
                    topology.backpressure.water.mark.high: 0.8
                    ## 低水位 －－ 当队列使用量低于这个量时， 认为可以解除阻塞
                    topology.backpressure.water.mark.low: 0.05
                    ## 阻塞比例 －－ 当阻塞task数／这个component并发 的比例高于这值时，触发反压
                    topology.backpressure.coordinator.trigger.ratio: 0.1

                    ## 反压采样周期， 单位ms
                    topology.backpressure.check.interval: 1000
                    ## 采样次数和采样比例， 即在连续4次采样中， 超过（不包含）（4 ＊0.75）次阻塞才能认为真正阻塞， 超过（不包含）(4 * 0.75)次解除阻塞才能认为是真正解除阻塞
                    topology.backpressure.trigger.sample.rate: 0.75
                    topology.backpressure.trigger.sample.number: 4
                 */
                config.put("topology.backpressure.enable", true);
                config.put("topology.backpressure.water.mark.high", 0.8);
                config.put("topology.backpressure.water.mark.low", 0.2);
                config.put("topology.backpressure.coordinator.trigger.ratio", 0.1);
                config.put("topology.backpressure.check.interval", 1000);
                config.put("topology.backpressure.trigger.sample.rate", 0.75);
                config.put("topology.backpressure.trigger.sample.number", 4);
            }

            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (AlreadyAliveException e) {
                logger.error("already alive", e);
            } catch (InvalidTopologyException e) {
                logger.error("invalid topology", e);
            }
        } else {
            String topName = "back-pressure-topology";

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topName, config, builder.createTopology());

            Utils.sleep(10000);
//            cluster.killTopology(topName);
            cluster.shutdown();
        }
    }
}
