package com.treefinance.binlog.bean;

import com.treefinance.binlog.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.Serializable;
import java.net.HttpURLConnection;

/**
 * @author personalc
 */
public abstract class FileSplit implements Serializable,Runnable {
    private static Logger LOG = Logger.getLogger(FileSplit.class);
    /**
     * 文件所在src
     */
    public String src;
    /**
     * 目标路径
     */
    public String dest;
    /**
     * 分段传输的开始位置
     */
    public long startPos;
    /**
     * 结束位置
     */
    public long endPos;
    /**
     * 线程编号
     */
    public int threadID;
    /**
     * 下载完成标志
     */
    public boolean over = false;
    /**
     * 当前分段结束标志
     */
    public boolean stop = false;
    /**
     * 文件工具
     */
    public FileUtil fileUtil;


    public FileSplit(String src, String dest, long startPos, long endPos, int threadID, String fileName) {
        this.src = src;
        this.dest = dest;
        this.startPos = startPos;
        this.endPos = endPos;
        this.threadID = threadID;
        fileUtil = new FileUtil(dest+File.separator+fileName, startPos);
    }

    public FileSplit() {

    }

    /**
     * 打印响应的头部信息
     *
     * @param conn HttpURLConnection
     */
    public void printResponseHeader(HttpURLConnection conn) {
        for (int i = 0; ; i++) {
            String fieldsName = conn.getHeaderFieldKey(i);
            if (fieldsName != null) {
                LOG.info(fieldsName + ":" + conn.getHeaderField(fieldsName));
            } else {
                break;
            }
        }
    }

    /**
     * 停止分段传输
     */
    public void setSplitTransStop() {
        stop = true;
    }
}
