package com.treefinance.binlog.process;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import com.treefinance.binlog.util.PropertiesUtil;
import org.apache.commons.io.FileUtils;

import static com.aliyuncs.http.FormatType.JSON;

import org.apache.log4j.Logger;

public class BinLogDownLoad {
    private static Logger LOG = Logger.getLogger(BinLogDownLoad.class);
    private static Properties properties = PropertiesUtil.getProperties();
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final String REGEX_PATTERN = properties.getProperty("REGEX_PATTERN");
    private static final String SAVE_PATH = properties.getProperty("SAVE_PATH");
    private static final String DB_INSTANCE_ID = properties.getProperty("DB_INSTANCE_ID");
    private static final String BINLOG_ACTION_NAME = properties.getProperty("BINLOG_ACTION_NAME");
    private static final int PAGE_SIZE = Integer.valueOf(properties.getProperty("PAGE_SIZE"));
    private static final String INSTANCE_ID = properties.getProperty("INSTANCE_ID");


    public static void main(String[] args) {
        // 创建DefaultAcsClient实例并初始化
        DefaultProfile profile = DefaultProfile.getProfile(
                REGION_ID,                     // 您的可用区ID
                ACCESS_KEY_ID,                 // 您的AccessKey ID
                ACCESS_SECRET);                // 您的AccessKey Secret
        IAcsClient client = new DefaultAcsClient(profile);

        // 创建API请求并设置参数
        DescribeBinlogFilesRequest binlogFilesRequest = new DescribeBinlogFilesRequest();
        binlogFilesRequest.setAcceptFormat(JSON);
        binlogFilesRequest.setActionName(BINLOG_ACTION_NAME);
        binlogFilesRequest.setDBInstanceId(DB_INSTANCE_ID);
        binlogFilesRequest.setStartTime("2018-04-01T17:00:00Z");
        binlogFilesRequest.setEndTime("2018-05-03T15:00:00Z");
        DescribeBinlogFilesResponse binlogFilesResponse;
        int totalRecordCount;
        int pageCount;
        int pageRecordCount;
        try {
            binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
            totalRecordCount = binlogFilesResponse.getTotalRecordCount();
            LOG.info("totalRecordCount: " + totalRecordCount);
            List<BinLogFile> binLogFiles = new ArrayList<>(totalRecordCount);
            pageCount = (int) Math.ceil((double) totalRecordCount / PAGE_SIZE);
            LOG.info("pageCount: " + pageCount);
            for (int i = 1; i <= pageCount; i++) {
                binlogFilesRequest.setPageNumber(i);
                binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
                pageRecordCount = binlogFilesResponse.getPageRecordCount();
                LOG.info("PageRecordCount: " + pageRecordCount);
                List<BinLogFile> items = binlogFilesResponse.getItems();
                binLogFiles.addAll(items);
            }
            List<Integer> fileNumList = getFileNumberFromUrl(binLogFiles);
            Stream<BinLogFile> filterBinLog = binLogFiles.parallelStream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID));
            long instanceLogSize = filterBinLog.count();
            int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
            if ((maxDiff + 1) == instanceLogSize) {
                binLogFiles.parallelStream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID)).forEach(binLogFile ->
                {
                    try {
                        LOG.info("file size: " + binLogFile.getFileSize());
                        LOG.info("checksum: " + binLogFile.getChecksum());
                        LOG.info("begin download binlog file :" + "[" + binLogFile.getDownloadLink() + "]");
                        FileUtils.copyURLToFile(new URL(binLogFile.getDownloadLink()),
                                new File(SAVE_PATH + File.separator + binLogFile.getHostInstanceID() + "-" + getFileNameFromUrl(binLogFile.getDownloadLink())));
                        LOG.info("download binlog file :" + binLogFile.getDownloadLink() + "successfully");
                    } catch (IOException e) {
                        LOG.info("download binlog file :" + "[ " + binLogFile.getDownloadLink() + "] failed with exception " + e.getMessage());
                    }
                });
            } else {
                LOG.info("the downloaded binlog files is not complete");
            }
        } catch (ClientException e) {
            LOG.info("download binlog files from aliyun failed with exception " + e.getMessage());
        }
    }

    /**
     * 从URL中解析下载的文件名
     *
     * @param link 下载连接
     * @return 文件名
     */
    private static String getFileNameFromUrl(String link) {
        String fileName = null;
        Pattern pattern = Pattern.compile(BinLogDownLoad.REGEX_PATTERN);
        Matcher matcher = pattern.matcher(link);
        if (matcher.find()) {
            fileName = matcher.group();
        } else {
            LOG.info("no fileName get from the link,please check the url or the regex pattern");
        }
        return fileName;
    }

    /**
     * 从URL中解析下载的文件编号
     *
     * @param binLogs 下载连接
     * @return 文件编号
     */
    private static List<Integer> getFileNumberFromUrl(List<BinLogFile> binLogs) {
        Pattern pattern = Pattern.compile(REGEX_PATTERN);
        return binLogs.stream()
                .map(binLogFile -> pattern.matcher(binLogFile.getDownloadLink()))
                .filter(Matcher::find)
                .map(matcher -> matcher.group(2))
                .map(Integer::valueOf)
                .sorted()
                .distinct()
                .collect(Collectors.toList());
    }
}