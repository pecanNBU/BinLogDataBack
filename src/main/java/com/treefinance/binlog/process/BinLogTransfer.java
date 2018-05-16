package com.treefinance.binlog.process;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;
import com.treefinance.binlog.bean.SplitInfo;
import com.treefinance.binlog.util.BinLogFileUtil;
import com.treefinance.binlog.util.DBInstanceUtil;
import com.treefinance.binlog.util.FileUtil;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * 根据条件下载指定实例binlog文件
 * @author personalc
 */
public class BinLogTransfer {
    private static Logger LOG = Logger.getLogger(BinLogTransfer.class);
    private static Properties properties = FileUtil.getProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private static final String SAVE_PATH = properties.getProperty("SAVE_PATH");
    private static final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private static final String START_TIME = properties.getProperty("START_TIME");
    private static final String END_TIME = properties.getProperty("END_TIME");
    private static String INSTANCE_ID = null;


    public static void main(String[] args) {
        // 创建DefaultAcsClient实例并初始化
        DefaultProfile profile = DefaultProfile.getProfile(
                REGION_ID,
                ACCESS_KEY_ID,
                ACCESS_SECRET);
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
            INSTANCE_ID = DBInstanceUtil.getBackInstanceId(dbInstance);
            List<BinLogFile> fileList = binLogFiles.parallelStream()
                    .filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID)).collect(Collectors.toList());
            long instanceLogSize = fileList.size();
            List<Integer> fileNumList = BinLogFileUtil.getFileNumberFromUrl(fileList, REGEX_PATTERN);
            fileNumList = fileNumList.stream().sorted().collect(Collectors.toList());
            for (Integer binId : fileNumList) {
                System.out.println(binId);
            }
            //判断文件编号是否连续
            if (fileList.size() > 0) {
                int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
                if (instanceLogSize == (maxDiff + 1)) {

                    fileList.parallelStream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID)).forEach(binLogFile ->

                    {
                        LOG.info("file size: " + binLogFile.getFileSize());
                        LOG.info("begin download binlog file :" + "[" + binLogFile.getDownloadLink() + "]");
                        String filePath = SAVE_PATH +
                                File.separator + dbInstance.getDBInstanceId()
                                + File.separator + binLogFile.getHostInstanceID();
                        File file = new File(filePath);
                        if (!file.exists()) {
                            file.mkdirs();
                        }
                        String fileName = BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN);
                        System.out.println(fileName);
                        SplitInfo splitInfo = new SplitInfo(binLogFile.getDownloadLink(),
                                filePath,
                                BinLogFileUtil.getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN), 3);
                        DownLoad downFile = new DownLoad(splitInfo);
                        downFile.startDown();
                        BinLogFileUtil.saveUrlToText(binLogFile, SAVE_PATH + File.separator + "downLink.txt");
                        LOG.info("download binlog file :" + binLogFile.getDownloadLink() + "successfully");
                        // TODO: 2018/5/15 此处添加将文件地址发送队列操作
                    });
                } else {
                    LOG.info("the downloaded binlog files is not complete");
                }
            }
        }
    }
}