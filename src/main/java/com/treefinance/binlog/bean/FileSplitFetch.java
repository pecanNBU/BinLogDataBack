package com.treefinance.binlog.bean;


import com.treefinance.binlog.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * 用于分段传输
 * 使用HTTP协议的首部字段实现
 * @author personalc
 */
public class FileSplitFetch  extends FileSplit implements Runnable{
    private static Logger LOG = Logger.getLogger(FileSplitFetch.class);

    public FileSplitFetch(String src, String tempPath, String dest, long startPos, long endPos, int threadID, String fileName) {
        super(src, tempPath, dest, startPos, endPos, threadID, fileName);
    }

    public FileSplitFetch() {
        super();
    }

    @Override
    public void run() {
        while (startPos < endPos && !stop) {
            try {
                URL ourl = new URL(src);
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
                over = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
