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

/**
 * 根据条件下载指定实例binlog文件
 */
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
    private static final String START_TIME = properties.getProperty("START_TIME");
    private static final String END_TIME = properties.getProperty("END_TIME");


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
        binlogFilesRequest.setStartTime(START_TIME);
        binlogFilesRequest.setEndTime(END_TIME);
        List<BinLogFile> binLogFiles = getBinLogFiles(client, binlogFilesRequest, profile);
        saveUrlToText(binLogFiles,SAVE_PATH+File.separator+"downLink.txt");
        List<Integer> fileNumList = getFileNumberFromUrl(binLogFiles);
        Stream<BinLogFile> filterBinLog = binLogFiles.parallelStream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID));
        long instanceLogSize = filterBinLog.count();
        //判断文件编号是否连续
        int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
        if (instanceLogSize == (maxDiff + 1)) {
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

    private static List<BinLogFile> getBinLogFiles(IAcsClient client, DescribeBinlogFilesRequest binlogFilesRequest, DefaultProfile profile) {
        DescribeBinlogFilesResponse binlogFilesResponse = null;
        try {
            binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
        } catch (ClientException e) {
            e.printStackTrace();
        }
        int totalRecordCount = 0;
        if (binlogFilesResponse != null) {
            totalRecordCount = binlogFilesResponse.getTotalRecordCount();
        }
        LOG.info("totalRecordCount: " + totalRecordCount);
        List<BinLogFile> binLogFiles = new ArrayList<>(totalRecordCount);
        int pageCount = (int) Math.ceil(totalRecordCount / PAGE_SIZE);
        LOG.info("pageCount: " + pageCount);
        for (int i = 1; i <= pageCount; i++) {
            binlogFilesRequest.setPageNumber(i);
            try {
                binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
            } catch (ClientException e) {
                e.printStackTrace();
            }
            int pageRecordCount = 0;
            if (binlogFilesResponse != null) {
                pageRecordCount = binlogFilesResponse.getPageRecordCount();
            }
            LOG.info("PageRecordCount: " + pageRecordCount);
            List<BinLogFile> items = binlogFilesResponse.getItems();
            binLogFiles.addAll(items);
        }
        return binLogFiles;
    }

    /**
     * 将binlog downloadLink 保存到filePath
     * @param binLogFiles 一批binlog文件
     * @param filePath 保存文件路径
     */
    private static void saveUrlToText(List<BinLogFile> binLogFiles,String filePath) {
        OutputStream fou = null;
        try {
           fou=new FileOutputStream(new File(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        for (BinLogFile binLogFile:binLogFiles){
            String downLoadLink=binLogFile.getDownloadLink();
            try {
                if (fou != null) {
                    fou.write((downLoadLink+"\n").getBytes());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (fou!=null){
            try {
                fou.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}