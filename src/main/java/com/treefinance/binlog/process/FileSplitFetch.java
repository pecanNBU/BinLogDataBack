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
    private String url;               // 文件所在url
    long startPos;          // 分段传输的开始位置
    long endPos;            // 结束位置
    private int threadID;             // 线程编号
    boolean downOver = false;         // 下载完成标志
    private boolean stop = false;     // 当前分段结束标志
    private FileUtil fileUtil;        // 文件工具

    FileSplitFetch(String url, long startPos, long endPos, int threadID, String fileName){
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
                httpConnection.setRequestProperty("RANGE", prop); //设置请求首部字段 RANGE

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
