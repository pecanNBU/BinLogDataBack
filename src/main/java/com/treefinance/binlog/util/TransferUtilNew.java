package com.treefinance.binlog.util;

import com.treefinance.binlog.bean.*;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author personalc
 */
public class TransferUtilNew {
    private static Logger LOG = Logger.getLogger(TransferUtilNew.class);

    /**
     * 文件不可访问
     */
    private static final int NO_ACCESS = -2;
    /**
     * 文件信息
     */
    private SplitInfo splitInfo;
    /**
     * 开始位置
     */
    private long startPos;
    /**
     * 结束位置
     */
    private long endPos;
    /**
     * 多线程分段传输的线程集合
     */
    private FileSplitAll fileSplits;
    /**
     * 是否第一次下载文件
     */
    private boolean firstDown = true;
    /**
     * 停止标志
     */
    private boolean stop = false;
    /**
     * 保存文件信息的临时文件
     */
    private static File infoFile;

    /**
     * 开始下载文件
     * 1. 获取文件长度
     * 2. 分割文件
     * 3. 实例化分段下载子线程
     * 4. 启动子线程
     * 5. 等待子线程的返回
     */

    public TransferUtilNew(SplitInfo splitInfo) {
        this.splitInfo = splitInfo;
        infoFile = new File(splitInfo.getTempPath() + File.separator + splitInfo.getSimpleName() + ".tmp");

        try {
            if (HDFSFileUtil.fileSystem.exists(new Path(splitInfo.getDestPath() + File.separator + splitInfo.getFileName()))) {
                firstDown = false;
                startPos = HDFSFileUtil.getFileSize(splitInfo.getDestPath() + File.separator + splitInfo.getFileName());
            } else {
                startPos = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        endPos = getFileSize();
    }

    public void startTrans(FileSplit fileSplit) {
        if (firstDown) {
            //文件长度
            long fileLen = getFileSize();
            if (fileLen == -1) {
                LOG.info("文件大小未知");
                return;
            } else if (fileLen == -2) {
                LOG.info("文件不可访问");
                return;
            } else {
                startPos = 0;
                endPos = fileLen;
            }
        }
        //启动分段下载子线程
        fileSplits = new FileSplitAll(splitInfo.getSrcPath(), splitInfo.getDestPath(), splitInfo.getDestPath(), startPos, endPos, 0,
                splitInfo.getFileName());
        LOG.info("Thread" + 0 + ", start= " + startPos + ",  end= " + endPos);
        new Thread(fileSplits).start();
        while (!stop) {
            sleep(3000);
            if (!fileSplits.over) {
                // 还存在未下载完成的线程
                break;
            } else {
                setStop();
            }
        }
        LOG.info("文件下载完成");
    }

    /**
     * 获取文件的大小
     *
     * @return 文件大小
     */
    private long getFileSize() {
        int len = -1;
        try {
            URL url = new URL(splitInfo.getSrcPath());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("User-Agent", "custom");

            int respCode = connection.getResponseCode();
            if (respCode >= 400) {
                LOG.info("Error Code : " + respCode);
                // 代表文件不可访问
                return NO_ACCESS;
            }

            String header;
            for (int i = 1; ; i++) {
                header = connection.getHeaderFieldKey(i);
                if (header != null) {
                    if ("Content-Length".equals(header)) {
                        len = Integer.parseInt(connection.getHeaderField(header));
                        break;
                    }
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }

        LOG.info("文件大小为" + len);
        System.out.println("file len:" + len);
        return len;
    }


    /**
     * 停止下载
     */
    public void setStop() {
        stop = true;
        fileSplits.setSplitTransStop();
    }

    /**
     * 休眠时间
     *
     * @param mills
     */
    public static void sleep(int mills) {
        try {
            Thread.sleep(mills);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
