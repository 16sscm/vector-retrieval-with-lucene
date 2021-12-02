package com.hiretual.search.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class GlobalPropertyUtils {

    private static final Logger logger = LoggerFactory.getLogger(GlobalPropertyUtils.class);
    static Map<String, String> parameters;

    public static int getIntValue(String key) {
        try {
            return Integer.parseInt(parameters.get(key));
        } catch (Exception e) {
            logger.error("int value required, fail to convert", e);
        }
        return -1;
    }

    public static String get(String key) {
        String value = parameters.get(key);
        if (value == null) {
            return "unknown error";
        }
        return value;
    }

     static{
        Properties props = new Properties();
        InputStream in = null;
        try {
            //String path = ClassUtils.getDefaultClassLoader().getResource("").getPath();
            //logger.info("payh = " + path);
            // System.out.println(System.getProperty("java.class.path"));
            ClassPathResource cpr = new ClassPathResource("global.properties");
            in = cpr.getInputStream();
            props.load(in);
            parameters = new HashMap<String, String>();
            Set<Object> keySet = props.keySet();
            for (Object key : keySet) {
                parameters.put((String) key, props.getProperty((String) key));
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("GlobalPropertyUtils file close error");
                }
            }
        }
    }


}
