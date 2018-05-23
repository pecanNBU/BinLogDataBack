package com.treefinance.binlog.process;

import com.treefinance.binlog.util.HDFSFileUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author personalc
 */
public class TransThread implements Serializable,Runnable {
    private static Logger LOG = Logger.getLogger(TransThread.class);
    /**
     * 文件所在src
     */
    public String src;
    /**
     * 目标路径
     */
    public String dest;
    /**
     * 文件名
     */
    public String fileName;
    /**
     * 分段传输的开始位置
     */
    public long startPos;
    /**
     * 结束位置
     */
    public long endPos;
    /**
     * 下载完成标志
     */
    public boolean over = false;



    public TransThread(String src, String dest, long startPos, long endPos, String fileName) {
        this.src = src;
        this.dest = dest;
        this.fileName = fileName;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    public TransThread() {

    }
    @Override
    public void run() {
        while (startPos < endPos && !over) {
            try {
                URL ourl = new URL(src);
                HttpURLConnection httpConnection = (HttpURLConnection) ourl.openConnection();
                //下载starPos以后的数据
                String prop = "bytes=" + startPos + "-";
                //设置请求首部字段 RANGE
                httpConnection.setRequestProperty("RANGE", prop);

                LOG.info(prop);

                InputStream input = httpConnection.getInputStream();
                byte[] b = new byte[1024];
                int bytes;
                int tries = 60;
                boolean recovered = false;

                Path dstPath = new Path(dest + File.separator + fileName);
                System.out.println(dstPath);
                FileSystem fs = HDFSFileUtil.fileSystem;
                if (!fs.exists(dstPath)) {
                    fs.create(dstPath).close();
                }
                while (!recovered && tries > 0) {
                    while ((((bytes = input.read(b))) > 0) && (startPos < endPos) && !over) {
                        try {
                            FSDataOutputStream out = fs.append(dstPath);
                            out.write(b, 0, bytes);
                            startPos += bytes;
                            recovered = true;
                            System.out.println(startPos);
                            out.close();
                        } catch (IOException e) {
                            if (e.getClass().getName().equals(RecoveryInProgressException.class.getName())) {
                                try {
                                    LOG.info("sleep 1000 millis and retry to append data to HDFS");
                                    Thread.sleep(1000);
                                    tries--;
                                } catch (InterruptedException e1) {
                                    e1.printStackTrace();
                                }
                            }
                        }
                    }
                }
                LOG.info("Thread " + Thread.currentThread().getName() + " is done");
                over = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
