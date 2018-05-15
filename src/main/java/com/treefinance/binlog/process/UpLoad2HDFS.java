package com.treefinance.binlog.process;

import com.treefinance.binlog.bean.HdfsFileInfo;
import org.apache.log4j.Logger;

import java.io.*;

public class UpLoad2HDFS {
    private static Logger LOG = Logger.getLogger(UpLoad2HDFS.class);
    HdfsFileInfo hdfsFileInfo;                     // 文件信息
    long[] startPos;                      // 开始位置
    long[] endPos;                        // 结束位置
    FileSplitUpLoadHDFS[] fileSplitUpLoadHDFS;     // 多线程分段传输的线程集合
    long fileLen;                          // 文件长度
    boolean firstDown = true;              // 是否第一次下载文件
    boolean stop = false;                  // 停止标志
    File infoFile;

    public UpLoad2HDFS(HdfsFileInfo hdfsFileInfo) {
        this.hdfsFileInfo = hdfsFileInfo;
        infoFile = new File(hdfsFileInfo.getSrc() + File.separator + hdfsFileInfo.getSimpleName() + "_up.tmp");
        if (infoFile.exists()) {
            firstDown = false;
            readInfo();
        } else {
            startPos = new long[hdfsFileInfo.getSplits()];
            endPos = new long[hdfsFileInfo.getSplits()];
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
    public void startUpLoad() {

        if (fileTransSet()) {

            //启动分段下载子线程

            fileSplitUpLoadHDFS = new FileSplitUpLoadHDFS[startPos.length];
            for (int i = 0; i < startPos.length; i++) {
                System.out.println(startPos[i] + " " + endPos[i]);
                fileSplitUpLoadHDFS[i] = new FileSplitUpLoadHDFS(hdfsFileInfo.getSrc(), hdfsFileInfo.getHdfsPath(), startPos[i], endPos[i], i, hdfsFileInfo.getFileName());
                LOG.info("Thread " + i + ", start= " + startPos[i] + ",  end= " + endPos[i]);
                new Thread(fileSplitUpLoadHDFS[i]).start();
            }

            //保存文件下载信息
            saveInfo();
            //循环判断所有文件是否下载完毕
            boolean breakWhile = false;
            while (!stop) {
                DownFile.sleep(1000);
                breakWhile = true;

                for (int i = 0; i < startPos.length; i++) {
                    if (!fileSplitUpLoadHDFS[i].upOver) {
                        breakWhile = false; // 还存在未下载完成的线程
                        break;
                    }
                }

                if (breakWhile)
                    break;
            }

            LOG.info("文件下载完成");
        }
    }

    private boolean fileTransSet() {
        if (firstDown) {
            fileLen = getFileSize();
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
                output.writeLong(fileSplitUpLoadHDFS[i].getStartPos());
                output.writeLong(fileSplitUpLoadHDFS[i].getEndPos());
            }
            output.close();
        } catch (FileNotFoundException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOG.info(e.getMessage());
            e.printStackTrace();
        }
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
     * 获取文件的大小
     *
     * @return
     */
    private long getFileSize() {
        File file = new File(hdfsFileInfo.getSrc() + File.separator + hdfsFileInfo.getFileName());
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
            fileSplitUpLoadHDFS[i].setSplitTransStop();
        }
    }

    public static void main(String args[]) {
        String dst = "hdfs://master1:8020/pc/";
        String src = "/Users/personalc/project/binlogfiles/test/rm-bp1h5j9w2o9335zsn/2018-05-12T00:00:00Z/4332347";
        HdfsFileInfo hdfsFileInfo = new HdfsFileInfo(src, dst, "mysql-bin.000672.tar", 3);
        UpLoad2HDFS upLoad2HDFS = new UpLoad2HDFS(hdfsFileInfo);
        upLoad2HDFS.startUpLoad();
    }
}
