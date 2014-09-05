//package org.expedia.test;
//
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.storm.hdfs.common.rotation.RotationAction;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
//public class AuditAndMoveFileAction implements RotationAction {
////        private static final Logger LOG = LoggerFactory.getLogger(AuditAndMoveFileAction.class);
////
////        private String destination;
////
////        public AuditAndMoveFileAction withDestination(String destDir) {
////                destination = destDir;
////                return this;
////        }
////
////        @Override
////        public void execute(FileSystem fileSystem, Path filePath) throws IOException {l
////                Path destPath = new Path(destination, filePath.getName());
////                LOG.info("Moving file {} to {}", filePath, destPath);
////                boolean success = fileSystem.rename(filePath, destPath);
////                return;
////        }
//}