package com.treefinance.binlog.process;

import com.treefinance.binlog.util.FileUtil;
import com.treefinance.binlog.util.HDFSFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;


/**
 * @author personalc
 */
public class FileSplitPush implements Runnable {
    private static Logger LOG = Logger.getLogger(FileSplitPush.class);
    /**
     * 文件所在url
     */
    private String src;
    /**
     * HDFS文件路径
     */
    private final String filePath;

    /**
     * 分段传输的开始位置
     */
    private long startPos;
    /**
     * 结束位置
     */
    private long endPos;
    /**
     * 线程编号
     */
    private int threadID;
    /**
     * 下载完成标志
     */
    boolean upOver = false;
    /**
     * 当前分段结束标志
     */
    private boolean stop = false;
    /**
     * 文件操作工具
     */
    private FileUtil fileUtil;
    /**
     * HDFS文件系统实例
     */
    private FileSystem fs = HDFSFileUtil.fileSystem;

    private static Configuration conf = HDFSFileUtil.configuration;

    private FileSystem local;


    FileSplitPush(String src, long startPos, long endPos, int threadID, String filePath) {
        super();
        this.src = src;
        this.startPos = startPos;
        this.endPos = endPos;
        this.threadID = threadID;
        this.filePath = filePath;
        fileUtil = new FileUtil(filePath, startPos);
    }

    @Override
    public void run() {
        try {
            Path dstPath = new Path(filePath);
            fs = HDFSFileUtil.fileSystem;
            local = FileSystem.getLocal(conf);
            if (!fs.exists(dstPath)) {
                fs.create(dstPath).close();
            }
            FSDataOutputStream out = fs.append(new Path(filePath));
            FSDataInputStream in = local.open(new Path(src ));
            byte[] buffer = new byte[1024];
            int byteRead;
            while ((byteRead = in.read(buffer)) > 0 && (startPos < endPos) && !stop) {
                out.write(buffer, 0, byteRead);
                startPos += byteRead;
            }
            LOG.info("Thread" + threadID + " is done");
            upOver = true;
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }


    public long getStartPos() {
        return startPos;
    }

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public long getEndPos() {
        return endPos;
    }

    public void setEndPos(long endPos) {
        this.endPos = endPos;
    }

    public int getThreadID() {
        return threadID;
    }

    public void setThreadID(int threadID) {
        this.threadID = threadID;
    }

    public boolean isUpOver() {
        return upOver;
    }

    public void setUpOver(boolean upOver) {
        this.upOver = upOver;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public FileUtil getFileUtil() {
        return fileUtil;
    }

    public void setFileUtil(FileUtil fileUtil) {
        this.fileUtil = fileUtil;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public static Logger getLOG() {
        return LOG;
    }

    public String getFilePath() {
        return filePath;
    }

    public static Configuration getConf() {
        return conf;
    }

    public FileSystem getLocal() {
        return local;
    }

    public void setLocal(FileSystem local) {
        this.local = local;
    }

    /**
     * 停止分段传输
     */
    void setSplitTransStop() {
        stop = true;
    }
}
