package com.treefinance.binlog.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties getProperties() {
        Properties ps = new Properties();
        try {
            InputStream is = new FileInputStream(FileUtil.loadResourceFile("aliyun.properties"));
            ps.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ps;
    }

}
