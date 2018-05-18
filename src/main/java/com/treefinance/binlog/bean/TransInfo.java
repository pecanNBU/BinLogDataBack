package com.treefinance.binlog.bean;

public class TransInfo {
    /**
     * 文件所在站点的url
     */
    private String srcPath;
    /**
     * 中间文件存放目录
     */
    private String midPath;
    /**
     * 文件保存的路径
     */
    private String destPath;
    /**
     * 文件的名字
     */
    private String fileName;

    public TransInfo(String srcPath, String midPath, String destPath, String fileName) {
        this.srcPath = srcPath;
        this.midPath = midPath;
        this.destPath = destPath;
        this.fileName = fileName;
    }

    public String getSrcPath() {
        return srcPath;
    }

    public void setSrcPath(String srcPath) {
        this.srcPath = srcPath;
    }

    public String getMidPath() {
        return midPath;
    }

    public void setMidPath(String midPath) {
        this.midPath = midPath;
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
}
