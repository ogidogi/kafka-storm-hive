package org.expedia.test;

import org.codehaus.jackson.map.ObjectMapper;
import org.expedia.test.bean.JsonField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class JacksonJsonParser extends BaseFunction {

        public static final char FIELD_DELIMITER = '\001';
        private static final Logger LOG = LoggerFactory.getLogger(JacksonJsonParser.class);

        @Override public void execute(TridentTuple objects, TridentCollector tridentCollector) {
                byte[] inputJson = objects.getBinaryByField("bytes");
                LOG.debug("Input byte array: " + String.valueOf(inputJson));

                try {
                        String inputJsonString = new String(inputJson, "UTF-8");
                        LOG.debug("Deserialized string from input bytes: " + inputJsonString);
                } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                }

                List<Object> outValues = new ArrayList<Object>();

                ObjectMapper mapper = new ObjectMapper();

                try {
                        JsonField jsonField = mapper.readValue(inputJson, JsonField.class);
                        jsonField.setFieldDelimiter(FIELD_DELIMITER);
                        outValues.add(jsonField);
                        LOG.debug("Output values: " + outValues);
                        tridentCollector.emit(outValues);
                } catch (IOException e) {
                        e.printStackTrace();
                }
        }
}
