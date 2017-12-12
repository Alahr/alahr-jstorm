package com.alahr.jstorm.common.properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertiesUtil {
    private final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    public Properties getProperties() {
        return properties;
    }

    private Properties properties;

    private boolean absolutePath = false;

    public PropertiesUtil(String fileName, boolean absolutePath) {
        if (absolutePath) {
            readPropertiesByAbsolutePath(fileName);
        } else {
            readProperties(fileName);
        }
    }

    private void readPropertiesByAbsolutePath(String fullFileName) {
        try {
            properties = new Properties();
            InputStream in = new BufferedInputStream(new FileInputStream(fullFileName));
            properties.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readProperties(String fileName) {
        try {
            properties = new Properties();
            InputStream fis = this.getClass().getResourceAsStream(fileName);
            properties.load(fis);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public Map getAllProperties() {
        Map map = new HashMap();
        Enumeration enu = properties.propertyNames();
        while (enu.hasMoreElements()) {
            String key = (String) enu.nextElement();
            String value = properties.getProperty(key);
            map.put(key, value);
        }
        return map;
    }

    public void printProperties() {
        properties.list(System.out);
    }
}
