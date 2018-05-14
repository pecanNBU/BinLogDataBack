package com.treefinance.binlog.process;

import com.treefinance.binlog.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;


public class FileSplitUpLoadHDFS implements Runnable {
    private static Logger LOG = Logger.getLogger(FileSplitUpLoadHDFS.class);
    private String src;               // 文件所在url
    private String dest;
    private long startPos;            // 分段传输的开始位置
    private long endPos;              // 结束位置
    private int threadID;             // 线程编号
    boolean upOver = false;         // 下载完成标志
    private boolean stop = false;     // 当前分段结束标志
    private FileUtil fileUtil;        // 文件工具
    private FileSystem fs;

    FileSplitUpLoadHDFS(String src, String dest, long startPos, long endPos, int threadID, String fileName) {
        super();
        this.src = src;
        this.dest = dest;
        this.startPos = startPos;
        this.endPos = endPos;
        this.threadID = threadID;
        fileUtil = new FileUtil(fileName, startPos);
    }

    @Override
    public void run() {
        Configuration conf = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(conf);
            FileSystem local = FileSystem.getLocal(conf);
            FSDataOutputStream out = hdfs.append(new Path(dest));
            FSDataInputStream in = local.open(new Path(src));
            byte buffer[] = new byte[1024];
            int byteRead = 0;
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

    public String getDest() {
        return dest;
    }

    public void setDest(String dest) {
        this.dest = dest;
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
    /**
     * 停止分段传输
     */
    void setSplitTransStop() {
        stop = true;
    }
}
