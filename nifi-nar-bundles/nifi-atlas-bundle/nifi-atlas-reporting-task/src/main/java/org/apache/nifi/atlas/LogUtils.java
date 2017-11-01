package org.apache.nifi.atlas;


import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LogUtils {
    private static final Logger logger = LoggerFactory.getLogger(LogUtils.class);

    static public void log (String title, Object object) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            String s = mapper.writerWithDefaultPrettyPrinter().writeValueAsString((object));
            logger.debug("------------------ {}", title);
            logger.debug(s);
            logger.debug("------------------");
        } catch (IOException e) {
            logger.warn("Pretty printing the object failed - silently ignoring.", e);
        }
    }
}
