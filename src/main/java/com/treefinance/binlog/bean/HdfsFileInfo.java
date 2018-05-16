package com.treefinance.binlog.bean;

import java.io.Serializable;

public class HdfsFileInfo implements Serializable {
    /**
     * 默认次数为5次
     */
    private static final int SPLIT_COUNT = 5;
    /**
     * 文件所在站点的url
     */
    private String src;
    /**
     * 文件保存的路径
     */
    private String hdfsPath;
    /**
     * 文件的名字
     */
    private String fileName;
    /**
     * 分段下载文件的次数
     */
    private int splits;

    public HdfsFileInfo() {
        this("", "", "", SPLIT_COUNT);
    }

    public HdfsFileInfo(String src, String hdfsPath, String fileName, int splits) {
        this.src = src;
        this.hdfsPath = hdfsPath;
        this.fileName = fileName;
        this.splits = splits;
    }

    public static int getSplitCount() {
        return SPLIT_COUNT;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
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

    public String getSimpleName() {
        /*String[] names = fileName.split("\\.");
        return names[0];*/
        return fileName.replace(".tar", "");
    }

}
