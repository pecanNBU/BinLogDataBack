package com.treefinance.binlog.process;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import com.treefinance.binlog.util.BinLogFileUtil;
import com.treefinance.binlog.util.DBInstanceUtil;
import com.treefinance.binlog.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * 根据条件下载指定实例binlog文件
 */
public class BinLogDownLoad {
    private static Logger LOG = Logger.getLogger(BinLogDownLoad.class);
    private static Properties properties = FileUtil.getProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private static final String SAVE_PATH = properties.getProperty("SAVE_PATH");
    private static final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private static final String START_TIME = properties.getProperty("START_TIME");
    private static final String END_TIME = properties.getProperty("END_TIME");
    private static String INSTANCE_ID;


    public static void main(String[] args) {
        // 创建DefaultAcsClient实例并初始化
        DefaultProfile profile = DefaultProfile.getProfile(
                REGION_ID,                     // 您的可用区ID
                ACCESS_KEY_ID,                 // 您的AccessKey ID
                ACCESS_SECRET);                // 您的AccessKey Secret
        IAcsClient client = new DefaultAcsClient(profile);

        // 创建API请求并设置参数
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setActionName(BINLOG_ACTION_NAME);
        binlogFilesRequest.setStartTime(START_TIME);
        binlogFilesRequest.setEndTime(END_TIME);
        List<DBInstance> instances = DBInstanceUtil.getAllPrimaryDBInstance();
        System.out.println(instances.size());
        for (DBInstance dbInstance : instances) {
            binlogFilesRequest.setDBInstanceId(dbInstance.getDBInstanceId());
            List<BinLogFile> binLogFiles = BinLogFileUtil.getBinLogFiles(client, binlogFilesRequest, profile);
            List<Integer> fileNumList = BinLogFileUtil.getFileNumberFromUrl(binLogFiles, REGEX_PATTERN);
            INSTANCE_ID = DBInstanceUtil.getBackInstanceId(dbInstance);
            Stream<BinLogFile> filterBinLog = binLogFiles.parallelStream()
                    .filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID));
            long instanceLogSize = filterBinLog.count();
            //判断文件编号是否连续
            if (fileNumList.size() > 0) {
                int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
                if (instanceLogSize == (maxDiff + 1)) {
                    binLogFiles.parallelStream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID)).forEach(binLogFile ->
                    {
                        try {
                            LOG.info("file size: " + binLogFile.getFileSize());
                            LOG.info("checksum: " + binLogFile.getChecksum());
                            LOG.info("begin download binlog file :" + "[" + binLogFile.getDownloadLink() + "]");
                            FileUtils.copyURLToFile(new URL(binLogFile.getDownloadLink()),
                                    new File(SAVE_PATH
                                            + File.separator + dbInstance.getDBInstanceId()

                                            + File.separator + START_TIME + File.separator
                                            + binLogFile.getHostInstanceID()
                                            + "-" + BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN)));
                            BinLogFileUtil.saveUrlToText(binLogFile, SAVE_PATH + File.separator + "downLink.txt");
                            LOG.info("download binlog file :" + binLogFile.getDownloadLink() + "successfully");
                        } catch (IOException e) {
                            LOG.info("download binlog file :" + "[ " + binLogFile.getDownloadLink() + "] failed with exception " + e.getMessage());
                        }
                    });
                } else {
                    LOG.info("the downloaded binlog files is not complete");
                }
            }
        }
    }
}