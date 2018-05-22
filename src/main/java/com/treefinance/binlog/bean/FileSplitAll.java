package com.treefinance.binlog.bean;


import com.treefinance.binlog.util.HDFSFileUtil;
import com.treefinance.binlog.util.TransferUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * 用于分段传输
 * 使用HTTP协议的首部字段实现
 *
 * @author personalc
 */
public class FileSplitAll extends FileSplit {
    private static Logger LOG = Logger.getLogger(FileSplitAll.class);
    private FileSystem fs = HDFSFileUtil.fileSystem;


    public FileSplitAll(String src, String tempPath, String dest, long startPos, long endPos, int threadID, String fileName) {
        super(src, tempPath, dest, startPos, endPos, threadID, fileName);
    }

    public FileSplitAll() {
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
                int tries = 60;
                boolean recovered = false;

                Path dstPath = new Path(dest + File.separator + fileName);
                System.out.println(dstPath);
                fs = HDFSFileUtil.fileSystem;
                if (!fs.exists(dstPath)) {
                    fs.create(dstPath).close();
                }
                while (!recovered && tries > 0) {
                    while ((((bytes = input.read(b))) > 0) && (startPos < endPos) && !stop) {
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
                LOG.info("Thread " + threadID + " is done");
                over = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
