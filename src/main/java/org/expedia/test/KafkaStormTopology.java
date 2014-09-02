package org.expedia.test;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class KafkaStormTopology {
        public static void main(String[] args) {
                TopologyBuilder builder = new TopologyBuilder();

                SpoutConfig spoutConf = new SpoutConfig(new ZkHosts("localhost:2181", "/brokers"), "test", "/kafkastorm", "KafkaSpout");
                spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
                spoutConf.forceFromStart = true;

                builder.setSpout("KafkaSpout", new KafkaSpout(spoutConf), 3);
                builder.setBolt("KafkaBolt", new PrinterBolt(), 3).shuffleGrouping("KafkaSpout");

                Config conf = new Config();
                // conf.setDebug(true);

                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("kafka-test", conf, builder.createTopology());

                Utils.sleep(60000);
                cluster.shutdown();
        }
}
