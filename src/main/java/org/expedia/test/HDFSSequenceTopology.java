package org.expedia.test;

import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;

import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.RawScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class HDFSSequenceTopology {

    public static final String KAFKA_TOPIC = "test_topic";
    public static final String ZKHOST = "localhost:2181";
    public static final String HDFS_OUT_PATH = "/user/hive/warehouse/target_rt/";
    // Use with partitioning table
    // public static final String HDFS_OUT_PATH = "/user/hive/warehouse/target_rt/processing_stage=RT/";
    public static final String HDFS_ROTATE_PATH = HDFS_OUT_PATH;
    public static final String HDFS_CLUSTER = "hdfs://localhost:8020";

    public static StormTopology buildTopology(String hdfsUrl) {
        TridentKafkaConfig tridentKafkaConfig = new TridentKafkaConfig(new ZkHosts(ZKHOST, "/brokers"), KAFKA_TOPIC);
        tridentKafkaConfig.scheme = new SchemeAsMultiScheme(new RawScheme());
        tridentKafkaConfig.startOffsetTime = -1; // forceStartOffsetTime(-1); //Read latest messages from Kafka

        TransactionalTridentKafkaSpout tridentKafkaSpout = new TransactionalTridentKafkaSpout(tridentKafkaConfig);

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("stream", tridentKafkaSpout);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(HDFS_OUT_PATH).withPrefix("trident").withExtension(".txt");
        FileRotationPolicy rotationPolicy = new FileSizeCountRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB, 10);
        HdfsState.Options seqOpts = new HdfsState.HdfsFileOptions().withFileNameFormat(fileNameFormat)
                .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|").withFields(new Fields("json")))
                .withRotationPolicy(rotationPolicy).withFsUrl(hdfsUrl)
                // .addRotationAction(new MoveFileAction().toDestination(HDFS_ROTATE_PATH));
                // .addRotationAction(new AddSuffixFileAction().withSuffix("-processed"));
                .addRotationAction(new MD5FileAction());
        StateFactory factory = new HdfsStateFactory().withOptions(seqOpts);

        stream.each(new Fields("bytes"), new JacksonJsonParser(), new Fields("json")).partitionPersist(factory, new Fields("json"),
                new HdfsUpdater(), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        conf.setMaxTaskParallelism(4);

        // conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka2hdfs", conf, buildTopology(HDFS_CLUSTER));

        Utils.sleep(180000);
        cluster.shutdown();
    }
}