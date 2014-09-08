package org.expedia.test;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class MD5FileAction implements RotationAction {

        private static final Logger LOG = LoggerFactory.getLogger(MD5FileAction.class);

        @Override public void execute(FileSystem fileSystem, Path filePath) throws IOException {

                FileChecksum chkSum = fileSystem.getFileChecksum(filePath);

                //try {

                String chksumFileName = filePath.getName().substring(0, filePath.getName().lastIndexOf('.')) + ".chksum";
                String chksumFilePath = filePath.getParent().toString();
                Path outPath = new Path(chksumFilePath + "/" + chksumFileName);

                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(outPath, true), "UTF-8"));
                LOG.info("Checksum for file {}: {}", filePath, chkSum.toString());
                //bufferedWriter.write(chkSum.hashCode());
                bufferedWriter.write(chkSum.toString());
                bufferedWriter.close();

                //} catch (Exception e) {
                //        LOG.error("File {} not found");
                //}

                return;
        }
}
