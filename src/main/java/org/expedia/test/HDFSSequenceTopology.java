package org.expedia.test;

import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DefaultSequenceFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.trident.sync.CountSyncPolicy;
import org.apache.storm.hdfs.trident.sync.SyncPolicy;

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
    @SuppressWarnings("unchecked")
    public static StormTopology buildTopology(String hdfsUrl) {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence", "key"), 1000, new Values("the cow jumped over the moon", 1l),
                new Values("the man went to the store and bought some candy", 2l), new Values("four score and seven years ago", 3l),
                new Values("how many apples can you eat", 4l), new Values("to be or not to be the person", 5l));
        spout.setCycle(true);
        TridentTopology topology = new TridentTopology();
        Stream stream = topology.newStream("spout1", spout);
        Fields hdfsFields = new Fields("sentence", "key");
        SyncPolicy syncPolicy = new CountSyncPolicy(2);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/tmp/hdfs-con").withPrefix("trident").withExtension(".seq");
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        HdfsState.Options seqOpts = new HdfsState.SequenceFileOptions().withFileNameFormat(fileNameFormat)
                .withSequenceFormat(new DefaultSequenceFormat("key", "sentence")).withRotationPolicy(rotationPolicy).withFsUrl(hdfsUrl)
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

        Utils.sleep(60000);
        cluster.shutdown();
    }
}