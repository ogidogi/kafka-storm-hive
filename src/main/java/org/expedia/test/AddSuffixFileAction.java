package org.expedia.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AddSuffixFileAction implements RotationAction {

        private static final Logger LOG = LoggerFactory.getLogger(AddSuffixFileAction.class);

        private String suffixFileName = "";

        public AddSuffixFileAction withSuffix(String suffixFileName) {
                this.suffixFileName = suffixFileName;
                return this;
        }

        @Override
        public void execute(FileSystem fileSystem, Path filePath) throws IOException {
                //String destFileName = filePath.getName();
                //Path destPath = filePath.getParent();
                Path destPath = filePath.suffix(suffixFileName);
                //Path destPath = new Path(destFileName, filePath.getName());
                LOG.info("Renamingl file {} to {}", filePath, destPath);
                boolean success = fileSystem.rename(filePath, destPath);
                return;
        }
}
