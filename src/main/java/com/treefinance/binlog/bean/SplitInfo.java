package com.treefinance.binlog.bean;

import java.io.Serializable;

/**
 * 要抓取文件的信息
 */
public class SplitInfo implements Serializable {
    /**
     * 默认次数为5次
     */
    private static final int SPLIT_COUNT = 5;
    /**
     * 文件所在站点的url
     */
    private String srcPath;
    /**
     * 临时文件目录
     */
    private String tempPath;
    /**
     * 文件保存的路径
     */
    private String destPath;
    /**
     * 文件的名字
     */
    private String fileName;
    /**
     * 分段下载文件的次数
     */
    private int splits;

    public SplitInfo() {
        this("", "", "", "", SPLIT_COUNT);
    }

    public SplitInfo(String srcPath, String tempPath, String destPath, String fileName, int splits) {
        this.srcPath = srcPath;
        this.tempPath = tempPath;
        this.destPath = destPath;
        this.fileName = fileName;
        this.splits = splits;
    }

    public static int getSplitCount() {
        return SPLIT_COUNT;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }

    public String getDestPath() {
        return destPath;
    }

    public void setDestPath(String destPath) {
        this.destPath = destPath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getSplits() {
        return splits;
    }

    public void setSplits(int splits) {
        this.splits = splits;
    }

    public String getTempPath() {
        return tempPath;
    }

    public void setTempPath(String tempPath) {
        this.tempPath = tempPath;
    }

    public String getSimpleName() {
        return fileName.replace(".tar", "");
    }



}