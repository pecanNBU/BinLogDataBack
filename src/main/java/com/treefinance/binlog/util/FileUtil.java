package com.treefinance.binlog.util;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * @author personalc
 */
public class FileUtil {
    private static Logger LOG = Logger.getLogger(FileUtil.class);
    private RandomAccessFile file;
    private long startPos;

    private static boolean strIsRight(String str) {
        return null != str && str.length() > 0;
    }

    /**
     * 加载配置文件
     *
     * @param resourceName 配置文件名
     * @return File
     */
    private static File loadResourceFile(String resourceName) {
        if (strIsRight(resourceName)) {
            URL url = ClassLoader.getSystemResource(resourceName);
            if (url != null) {
                File file = new File(url.getPath());
                LOG.info("Load resource file:" + url.getPath() + " successful!");
                return file;
            } else {
                LOG.error("Resource file:" + resourceName + " is not exist!");
                System.exit(1);
            }
        } else {
            LOG.error("The file name is not valid!");
        }
        return null;
    }

    /**
     * 读取配置文件
     *
     * @return Properties
     */
    public static Properties getProperties() {
        Properties ps = new Properties();
        try {
            InputStream is = new FileInputStream(loadResourceFile("instance.properties"));
            ps.load(is);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ps;
    }

    /**
     * 设置文件存储开始位置
     *
     * @param filePath 文件名
     * @param startPos 文件存储的起始位置
     */
    public FileUtil(String filePath, long startPos) {
        try {
            file = new RandomAccessFile(filePath, "rw");
            this.startPos = startPos;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            file.seek(startPos);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写数据操作
     *
     * @param data  字节数组
     * @param start 开始位置
     * @param len   结束位置
     * @return len 表示是否写入成功
     */
    public synchronized int write(byte[] data, int start, int len) {
        int res = -1;
        try {
            file.write(data, start, len);
            res = len;
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }
        return res;
    }
}