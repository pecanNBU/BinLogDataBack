import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import org.apache.commons.io.FileUtils;

import static com.aliyuncs.http.FormatType.JSON;

import org.apache.log4j.Logger;

public class Demo {
    private static Logger LOG = Logger.getLogger(Demo.class);
    private static final String REGION_ID = "cn-hangzhou";
    private static final String ACCESS_KEY_ID = "LTAIAfBoz0Wz5O6L";
    private static final String ACCESS_SECRET = "WGlWEscL3u5rfFrokhYle4jFXsXBv9";
    private static final String REGEX_PATTERN = "(mysql-bin\\.)(.*)(\\.tar)";
    private static final String SAVE_PATH = "/Users/personalc/project/binlogfiles";
    private static final String DB_INSTANCE_ID = "rm-bp11gox03jgt2ullb";
    private static final String BINLOG_ACTION_NAME = "DescribeBinlogFiles";
    private static final int PAGE_SIZE = 30;
    private static final String INSTANCE_ID = "3691577";

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
            System.out.println("totalRecordCount: " + totalRecordCount);
            List<BinLogFile> binLogFiles = new ArrayList<>(totalRecordCount);
            pageCount = (int) Math.ceil((double) totalRecordCount / PAGE_SIZE);
            System.out.println("pageCount: " + pageCount);
            for (int i = 1; i <= pageCount; i++) {
                binlogFilesRequest.setPageNumber(i);
                binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
                pageRecordCount = binlogFilesResponse.getPageRecordCount();
                System.out.println("PageRecordCount: " + pageRecordCount);
                List<BinLogFile> items = binlogFilesResponse.getItems();
                binLogFiles.addAll(items);
            }
            List<Integer> fileNumList = getFileNumberFromUrl(binLogFiles);
            long instanceLogSize = binLogFiles.stream().filter(binLogFile -> binLogFile.getHostInstanceID().equals(INSTANCE_ID)).count();
            int maxDiff = Math.abs(fileNumList.get(0) - fileNumList.get(fileNumList.size() - 1));
            if ((maxDiff + 1) == instanceLogSize) {
                binLogFiles.parallelStream().forEach(binLogFile ->
                {
                    try {
                        LOG.info("begin download binlog file :"+binLogFile.getDownloadLink());
                        FileUtils.copyURLToFile(new URL(binLogFile.getDownloadLink()),
                                new File(SAVE_PATH + File.separator + binLogFile.getHostInstanceID() + "-" + getFileNameFromUrl(binLogFile.getDownloadLink())));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else {
                LOG.info("the downloaded binlog files is not complete");
            }
        } catch (ClientException e) {
            e.printStackTrace();
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
        Pattern pattern = Pattern.compile(Demo.REGEX_PATTERN);
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