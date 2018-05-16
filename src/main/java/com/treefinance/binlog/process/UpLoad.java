package com.treefinance.binlog.process;

import com.treefinance.binlog.bean.SplitInfo;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * @author personalc
 */
public class UpLoad {
    private static Logger LOG = Logger.getLogger(UpLoad.class);
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
    private FileSplitPush[] fileSplitPushes;
    /**
     * 是否第一次下载文件
     */
    private boolean firstDown = true;
    /**
     * 停止标志
     */
    private boolean stop = false;
    private File infoFile;


    private UpLoad(SplitInfo splitInfo) {
        this.splitInfo = splitInfo;
        infoFile = new File(splitInfo.getSrcPath() + File.separator + splitInfo.getSimpleName() + "_up.tmp");
        if (infoFile.exists()) {
            firstDown = false;
            DownLoad.readInfo();
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
     *
     * @throws IOException
     */
    private void startUpLoad() {

        if (fileTransSet()) {

            //启动分段下载子线程
            fileSplitPushes = new FileSplitPush[startPos.length];
            for (int i = 0; i < startPos.length; i++) {
                System.out.println(startPos[i] + " " + endPos[i]);
                fileSplitPushes[i] = new FileSplitPush(splitInfo.getSrcPath(), startPos[i], endPos[i], i, splitInfo.getFileName());
                LOG.info("Thread " + i + ", start= " + startPos[i] + ",  end= " + endPos[i]);
                new Thread(fileSplitPushes[i]).start();
            }

            //保存文件下载信息
            saveInfo();
            //循环判断所有文件是否下载完毕
            boolean breakWhile;
            while (!stop) {
                DownLoad.sleep(60000);
                breakWhile = true;
                for (int i = 0; i < startPos.length; i++) {
                    if (!fileSplitPushes[i].upOver) {
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
    }

    private boolean fileTransSet() {
        if (firstDown) {
            long fileLen = getFileSize();
            if (fileLen == -1) {
                LOG.info("文件大小未知");
                return false;
            } else if (fileLen == -2) {
                LOG.info("文件不可访问");
                return false;
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
        return true;
    }

    /**
     * 保存文件下载信息
     */
    private void saveInfo() {
        try {
            DataOutputStream output = new DataOutputStream(new FileOutputStream(infoFile));
            output.writeInt(startPos.length);
            for (int i = 0; i < startPos.length; i++) {
                output.writeLong(fileSplitPushes[i].getStartPos());
                output.writeLong(fileSplitPushes[i].getEndPos());
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
     * @return
     */
    private long getFileSize() {
        File file = new File(splitInfo.getSrcPath() + File.separator + splitInfo.getFileName());
        Boolean flag = file.exists();
        System.out.println(flag);
        return file.length();
    }

    /**
     * 停止下载
     */
    public void setStop() {
        stop = true;
        for (int i = 0; i < startPos.length; i++) {
            fileSplitPushes[i].setSplitTransStop();
        }
    }

    public static void main(String args[]) {
        String src = "/Users/personalc/project/binlogfiles/test/rm-bp1h5j9w2o9335zsn/4332347";
        String dst = "hdfs://master1:8020/pc/";
        String fileName = "mysql-bin.000672.tar";
        SplitInfo hdfsFileInfo = new SplitInfo(src, dst, fileName, 1);
        UpLoad upLoad = new UpLoad(hdfsFileInfo);
        upLoad.startUpLoad();
    }
}
