package com.treefinance.binlog.bean;

import java.io.Serializable;

/**
 * 要抓取文件的信息
 * @author personalc
 */
public class TransInfo implements Serializable {
    /**
     * 文件所在站点的url
     */
    private String srcPath;
    /**
     * 文件保存的路径
     */
    private String destPath;
    /**
     * 文件名
     */
    private String fileName;


    public TransInfo(String srcPath, String destPath, String fileName) {
        this.srcPath = srcPath;
        this.destPath = destPath;
        this.fileName = fileName;
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


    public String getSimpleName() {
        return fileName.replace(".tar", "");
    }


}