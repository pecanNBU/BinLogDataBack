package com.treefinance.binlog.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;

public class HDFSFileUtil {
    private static Logger LOG = Logger.getLogger(BinLogFileUtil.class);
    public static Configuration configuration = null;
    public static FileSystem fileSystem = null;
    public static String hdfsPath = null;

    static {
        try {
            if (null == configuration) {
                configuration = new Configuration();
            }
            if (null == fileSystem) {
                fileSystem = FileSystem.get(URI.create(hdfsPath), configuration);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件上传
     *
     * @param src  本地路径
     * @param des  HDFS路径
     * @param conf HDFS配置
     * @return 上传是否成功
     */
    public static boolean put2HDFS(String src, String des, Configuration conf) {
        Path desPath = new Path(des);
        try {
            //FileSystem fs = desPath.getFileSystem(conf);
//          FileSystem hdfs = FileSystem.get( URI.create(dst), conf) ;
            fileSystem.copyFromLocalFile(false, new Path(src), desPath);
            /*FileSystem fs = FileSystem.get(URI.create(des), conf);
            FSDataOutputStream out = fs.append(new Path(des));*/

        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 文件下载
     *
     * @param src  本地路径
     * @param dst  HDFS路径
     * @param conf HDFS配置
     * @return 下载是否成功
     */
    public static boolean getFromHDFS(String src, String dst, Configuration conf) {
        Path dstPath = new Path(dst);
        try {
            //FileSystem fs = dstPath.getFileSystem(conf);
            fileSystem.copyToLocalFile(false, new Path(src), dstPath);
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 文件检测并删除
     *
     * @param path HDFS文件路径
     * @param conf HDFS配置
     * @return 检测及删除结果
     */
    public static boolean checkAndDel(final String path, Configuration conf) {
        Path dstPath = new Path(path);
        try {
            //FileSystem dhfs = dstPath.getFileSystem(conf);
            if (fileSystem.exists(dstPath)) {
                fileSystem.delete(dstPath, true);
            } else {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


 /*
    public static void main(String[] args) {
        String dst = "hdfs://master1:8020/pc/";
        String src = "/Users/personalc/project/binlogfiles/rm-bp11gox03jgt2ullb";
        boolean status = false;

        Configuration conf = new Configuration();
        status = put2HDFS(src, dst, conf);
        System.out.println("status=" + status);

        *//*src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb";
        dst = "/tmp/output";
        status = getFromHDFS(src, dst, conf);
        System.out.println("status=" + status);

        src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb";
        dst = "/tmp/output/loadtable.rb";
        status = checkAndDel(dst, conf);
        System.out.println("status=" + status);*//*
    }*/

    /**
     * @param src
     * @param dest
     * @return
     * @throws Exception
     */
    public static long upload2HDFSinOffset(String src, String dest, HttpServletRequest request) throws Exception {

        if (src == null || src.equals(""))
            return 0;
        long length = 0;
        LOG.info("create files in hdfs");
        Path p = new Path(dest);
        try {
            if (!fileSystem.exists(p)) {
                FSDataOutputStream fsOutputStream = null;
                // 偏移量为0，首次上传，create方法;

                if (!fileSystem.exists(new Path(dest))) {
                    fsOutputStream = fileSystem.create(new Path(dest));
                    fileSystem.close();
                } else {
                    fsOutputStream = fileSystem.create(new Path(dest));
                }

                ServletInputStream fos = request.getInputStream();
                byte[] buffer = new byte[1024];
                int len = 0;

                while ((len = fos.read(buffer)) != -1) {
                    fsOutputStream.write(buffer, 0, len);
                    length += len;
                }
                fsOutputStream.flush();
                fsOutputStream.close();
                fos.close();
                fileSystem.close();
                System.out.println("HDFSHandler if return :" + length);
                return length;
            } else {
                if (!fileSystem.exists(new Path(dest))) {
                    fileSystem.create(new Path(dest));
                    fileSystem.close();
                }
                FSDataOutputStream fsOutputStream2;
                fsOutputStream2 = fileSystem.append(new Path(dest));
                ServletInputStream fos2 = request.getInputStream();
                byte[] buffer = new byte[1024];
                int len = 0;
                while ((len = fos2.read(buffer)) != -1) {
                    fsOutputStream2.write(buffer, 0, len);
                    length += len;
                }
                fsOutputStream2.flush();
                fsOutputStream2.close();
                fos2.close();
                fileSystem.close();
                return length;
            }
        } catch (Exception e) {// 用户中断上传，传回已接收到的文件长度（记录在偏移量表中，以待用户断线续传时传给用户）
            return length;
        }
    }
}
