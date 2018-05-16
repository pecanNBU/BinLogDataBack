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
    private String url;
    /**
     * 文件保存的路径
     */
    private String filePath;
    /**
     * 文件的名字
     */
    private String fileName;
    /**
     * 分段下载文件的次数
     */
    private int splits;

    public SplitInfo() {
        this("", "", "", SPLIT_COUNT);
    }

    public SplitInfo(String url, String filePath, String fileName, int splits) {
        this.url = url;
        this.filePath = filePath;
        this.fileName = fileName;
        this.splits = splits;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
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
        return fileName.replace(".tar", "");
    }


}