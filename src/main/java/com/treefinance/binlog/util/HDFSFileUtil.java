package com.treefinance.binlog.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class HDFSFileUtil {
    public static Configuration configuration = null;
    public static FileSystem fileSystem = null;
    public static String hdfsPath=null;

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
    public static boolean put2HSFS(String src, String des, Configuration conf) {
        Path desPath = new Path(des);
        try {
            FileSystem fs = desPath.getFileSystem(conf);
//          FileSystem hdfs = FileSystem.get( URI.create(dst), conf) ;
            fs.copyFromLocalFile(false, new Path(src), desPath);
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
            FileSystem fs = dstPath.getFileSystem(conf);
            fs.copyToLocalFile(false, new Path(src), dstPath);
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
            FileSystem dhfs = dstPath.getFileSystem(conf);
            if (dhfs.exists(dstPath)) {
                dhfs.delete(dstPath, true);
            } else {
                return false;
            }
        } catch (IOException ie) {
            ie.printStackTrace();
            return false;
        }
        return true;
    }


    /**
     * @param args
     */
    public static void main(String[] args) {
        String dst = "hdfs://master1:8020/pc/";
        String src = "/Users/personalc/project/binlogfiles/rm-bp11gox03jgt2ullb";
        boolean status = false;

        Configuration conf = new Configuration();
        status = put2HSFS(src, dst, conf);
        System.out.println("status=" + status);

        /*src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb";
        dst = "/tmp/output";
        status = getFromHDFS(src, dst, conf);
        System.out.println("status=" + status);

        src = "hdfs://xcloud:9000/user/xcloud/out/loadtable.rb";
        dst = "/tmp/output/loadtable.rb";
        status = checkAndDel(dst, conf);
        System.out.println("status=" + status);*/
    }
}
