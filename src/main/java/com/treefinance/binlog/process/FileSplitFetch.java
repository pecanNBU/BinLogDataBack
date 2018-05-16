package com.treefinance.binlog.process;


import com.treefinance.binlog.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * 用于分段传输
 * 使用HTTP协议的首部字段实现
 */
public class FileSplitFetch implements Runnable {
    private static Logger LOG = Logger.getLogger(FileSplitFetch.class);
    /**
     * 文件所在url
     */
    private String url;
    /**
     * 分段传输的开始位置
     */
    long startPos;
    /**
     * 结束位置
     */
    long endPos;
    /**
     * 线程编号
     */
    private int threadID;
    /**
     * 下载完成标志
     */
    boolean downOver = false;
    /**
     * 当前分段结束标志
     */
    private boolean stop = false;
    /**
     * 文件工具
     */
    private FileUtil fileUtil;


    FileSplitFetch(String url, long startPos, long endPos, int threadID, String fileName) {
        super();
        this.url = url;
        this.startPos = startPos;
        this.endPos = endPos;
        this.threadID = threadID;
        fileUtil = new FileUtil(fileName, startPos);
    }


    @Override
    public void run() {
        while (startPos < endPos && !stop) {
            try {
                URL ourl = new URL(url);
                HttpURLConnection httpConnection = (HttpURLConnection) ourl.openConnection();
                String prop = "bytes=" + startPos + "-";
                //设置请求首部字段 RANGE
                httpConnection.setRequestProperty("RANGE", prop);

                LOG.info(prop);

                InputStream input = httpConnection.getInputStream();
                byte[] b = new byte[1024];
                int bytes;
                while ((((bytes = input.read(b))) > 0) && (startPos < endPos) && !stop) {
                    startPos += fileUtil.write(b, 0, bytes);
                }

                LOG.info("Thread" + threadID + " is done");
                downOver = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
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
    void setSplitTransStop() {
        stop = true;
    }


}
