package org.expedia.test;

import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import storm.trident.tuple.TridentTuple;

public class FileSizeCountRotationPolicy extends FileSizeRotationPolicy {

        private long maxCounter = 1000L;
        private long curCounter = 0L;

        public FileSizeCountRotationPolicy(float count, Units units, long maxCounter) {
                super(count, units);
                this.maxCounter = maxCounter;
        }

        @Override
        public boolean mark(TridentTuple tuple, long offset) {
                boolean result = super.mark(tuple, offset);

                curCounter++;

                return curCounter >= maxCounter ? true : result;
        }

        @Override
        public void reset() {
                super.reset();
                curCounter = 0L;
        }
}
