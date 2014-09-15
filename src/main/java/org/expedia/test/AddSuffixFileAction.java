package org.expedia.test;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.common.rotation.RotationAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AddSuffixFileAction implements RotationAction {

    /**
     * 
     */
    private static final long serialVersionUID = 3014745235064765456L;

    private static final Logger LOG = LoggerFactory.getLogger(AddSuffixFileAction.class);

    private String suffixFileName = "";

    public AddSuffixFileAction withSuffix(String suffixFileName) {
        this.suffixFileName = suffixFileName;
        return this;
    }

    @Override
    public void execute(FileSystem fileSystem, Path filePath) throws IOException {
        Path destPath = filePath.suffix(suffixFileName);
        LOG.info("Renaming file {} to {}", filePath, destPath);
        fileSystem.rename(filePath, destPath);
        return;
    }
}
