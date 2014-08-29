package org.expedia.test;

import backtype.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DefaultSequenceFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.trident.sync.CountSyncPolicy;
import org.apache.storm.hdfs.trident.sync.SyncPolicy;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HDFSSequenceTopology {
        public static StormTopology buildTopology(String hdfsUrl) {
                TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(new ZkHosts("localhost:2181", "/brokers"), "test_topic");
                tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

                TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);
                //spout.setCycle(true);

                TridentTopology topology = new TridentTopology();

                Stream stream = topology.newStream("spout1", tridentKafkaSpout);
                Fields hdfsFields = new Fields("str");
                //SyncPolicy syncPolicy = new CountSyncPolicy(2);
                FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/hdfs-con").withPrefix("trident")
                        .withExtension(".txt");
                FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
                HdfsState.Options seqOpts = new HdfsState.HdfsFileOptions().withFileNameFormat(fileNameFormat)
                        .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|").withFields(new Fields("str")))
                        .withRotationPolicy(rotationPolicy)
                        .withFsUrl(hdfsUrl)
                        .addRotationAction(new MoveFileAction().toDestination("/tmp/hdfs-con2/"));
                StateFactory factory = new HdfsStateFactory().withOptions(seqOpts);
                stream.partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());
                return topology.build();
        }

        public static void main(String[] args) throws Exception {
                Config conf = new Config();
                conf.setMaxSpoutPending(5);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordCounter", conf, buildTopology("hdfs://localhost:8020"));

                Utils.sleep(180000);
                cluster.shutdown();
        }
}