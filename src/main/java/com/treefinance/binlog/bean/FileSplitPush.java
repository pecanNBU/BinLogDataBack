package com.treefinance.binlog.bean;

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
public class FileSplitPush extends FileSplit implements Runnable {
    private static Logger LOG = Logger.getLogger(FileSplitPush.class);

    private FileSystem fs = HDFSFileUtil.fileSystem;

    private static Configuration conf = HDFSFileUtil.configuration;

    private FileSystem local;


    public FileSplitPush(String src, String dest, long startPos, long endPos, int threadID, String fileName) {
        super(src, dest, startPos, endPos, threadID, fileName);
    }

    @Override
    public void run() {
        try {
            Path dstPath = new Path(dest);
            fs = HDFSFileUtil.fileSystem;
            local = FileSystem.getLocal(conf);
            if (!fs.exists(dstPath)) {
                fs.create(dstPath).close();
            }
            FSDataOutputStream out = fs.append(new Path(dest));
            FSDataInputStream in = local.open(new Path(src ));
            byte[] buffer = new byte[1024];
            int byteRead;
            while ((byteRead = in.read(buffer)) > 0 && (startPos < endPos) && !stop) {
                out.write(buffer, 0, byteRead);
                startPos += byteRead;
            }
            LOG.info("Thread" + threadID + " is done");
            over = true;
            in.close();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
