package com.treefinance.binlog.process;

import com.treefinance.binlog.bean.TransInfo;
import com.treefinance.binlog.util.HDFSFileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author personalc
 */
class TransferProcess {
    private static Logger LOG = Logger.getLogger(TransferProcess.class);
    private static final int FILE_SIZE_NOT_KNOWN = -1;
    private static final int FILE_NOT_ACCESSIBLE = -2;
    private static final int HTTP_CONNECTION_RESPONSE_CODE = 400;

    ThreadPoolExecutor executors = new ThreadPoolExecutor(5, 10, 3, TimeUnit.SECONDS, new LinkedBlockingDeque<>());

    /**
     * 文件信息
     */
    private TransInfo transInfo;
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
    private TransThread transThread;
    /**
     * 是否第一次下载文件
     */
    private boolean firstDown = true;
    /**
     * 停止标志
     */
    private boolean stop = false;

    /**
     * 开始下载文件
     * 1. 获取文件长度
     * 2. 分割文件
     * 3. 实例化分段下载子线程
     * 4. 启动子线程
     * 5. 等待子线程的返回
     */

    public TransferProcess(TransInfo transInfo) {
        this.transInfo = transInfo;
        try {
            if (HDFSFileUtil.fileSystem.exists(new Path(transInfo.getDestPath() + File.separator + transInfo.getFileName()))) {
                firstDown = false;
                startPos = HDFSFileUtil.getFileSize(transInfo.getDestPath() + File.separator + transInfo.getFileName());
            } else {
                startPos = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        endPos = getFileSize();
    }

    public void startTrans() {
        if (firstDown) {
            //文件长度
            long fileLen = getFileSize();
            if (fileLen == FILE_SIZE_NOT_KNOWN) {
                LOG.info("文件大小未知");
                return;
            } else if (fileLen == FILE_NOT_ACCESSIBLE) {
                LOG.info("文件不可访问");
                return;
            } else {
                startPos = 0;
                endPos = fileLen;
            }
        }
        transThread = new TransThread(transInfo.getSrcPath(), transInfo.getDestPath(), startPos, endPos,
                transInfo.getFileName());
        LOG.info("Thread :" + Thread.currentThread().getName() + ", start= " + startPos + ",  end= " + endPos);
        executors.execute(transThread);
        //new Thread(transThread).start();
        while (!stop) {
            sleep(3000);
            if (!transThread.over) {
                // 还存在未下载完成的线程
                break;
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
            URL url = new URL(transInfo.getSrcPath());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("User-Agent", "custom");

            int respCode = connection.getResponseCode();
            if (respCode >= HTTP_CONNECTION_RESPONSE_CODE) {
                LOG.info("Error Code : " + respCode);
                // 代表文件不可访问
                return FILE_NOT_ACCESSIBLE;
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
     * 休眠时间
     *
     * @param mills 休眠时间
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
