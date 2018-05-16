package com.treefinance.binlog.process;


import com.treefinance.binlog.bean.SplitInfo;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;


/**
 * @author personalc
 */
public class DownFile {
    private static Logger LOG = Logger.getLogger(DownFile.class);
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
    private long[] startPos;
    /**
     * 结束位置
     */
    private long[] endPos;
    /**
     * 多线程分段传输的线程集合
     */
    private FileSplitFetch[] fileSplitFetchs;
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
    private File infoFile;


    public DownFile(SplitInfo splitInfo) {
        this.splitInfo= splitInfo;
        infoFile = new File(splitInfo.getDestPath() + File.separator + splitInfo.getSimpleName() + ".tmp");
        if (infoFile.exists()) {
            firstDown = false;
            readInfo();
        } else {
            startPos = new long[splitInfo.getSplits()];
            endPos = new long[splitInfo.getSplits()];
        }
    }

    /**
     * 开始下载文件
     * 1. 获取文件长度
     * 2. 分割文件
     * 3. 实例化分段下载子线程
     * 4. 启动子线程
     * 5. 等待子线程的返回
     */
    void startDown() {

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
                // 设置每次分段下载的开始位置
                for (int i = 0; i < startPos.length; i++) {
                    startPos[i] = i * (fileLen / startPos.length);
                }
                //设置每次分段下载的结束位置
                for (int i = 0; i < endPos.length - 1; i++) {
                    endPos[i] = startPos[i + 1];
                }
                endPos[endPos.length - 1] = fileLen;
            }
        }

        //启动分段下载子线程
        fileSplitFetchs = new FileSplitFetch[startPos.length];
        for (int i = 0; i < startPos.length; i++) {
            System.out.println(startPos[i] + " " + endPos[i]);
            fileSplitFetchs[i] = new FileSplitFetch(splitInfo.getSrcPath(), startPos[i], endPos[i], i,
                    splitInfo.getDestPath() + File.separator + splitInfo.getFileName());
            LOG.info("Thread" + i + ", start= " + startPos[i] + ",  end= " + endPos[i]);
            new Thread(fileSplitFetchs[i]).start();
        }

        //保存文件下载信息
        saveInfo();
        //循环判断所有文件
        // 是否下载完毕
        boolean breakWhile;
        while (!stop) {
            sleep(500);
            breakWhile = true;
            for (int i = 0; i < startPos.length; i++) {
                if (!fileSplitFetchs[i].downOver) {
                    // 还存在未下载完成的线程
                    breakWhile = false;
                    break;
                }
            }
            if (breakWhile) {
                break;
            }
        }
        LOG.info("文件下载完成");
    }

    /**
     * 保存文件下载信息
     */
    private void saveInfo() {
        try {
            DataOutputStream output = new DataOutputStream(new FileOutputStream(infoFile));
            output.writeInt(startPos.length);
            for (int i = 0; i < startPos.length; i++) {
                output.writeLong(fileSplitFetchs[i].startPos);
                output.writeLong(fileSplitFetchs[i].endPos);
            }
            output.close();
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }
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
     * 读取文件下载保存的信息
     */
    private void readInfo() {
        try {
            DataInputStream input = new DataInputStream(new FileInputStream(infoFile));
            int count = input.readInt();
            startPos = new long[count];
            endPos = new long[count];
            for (int i = 0; i < count; i++) {
                startPos[i] = input.readLong();
                endPos[i] = input.readLong();
            }
            input.close();
        } catch (FileNotFoundException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 停止下载
     */
    public void setStop() {
        stop = true;
        for (int i = 0; i < startPos.length; i++) {
            fileSplitFetchs[i].setSplitTransStop();
        }
    }

    public static void sleep(int mills) {
        try {
            Thread.sleep(mills);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}

