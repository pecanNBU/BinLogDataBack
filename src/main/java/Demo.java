import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeBinlogFilesResponse.BinLogFile;
import org.apache.commons.io.FileUtils;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.ChecksumType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import static com.aliyuncs.http.FormatType.JSON;

import org.apache.log4j.Logger;

public class Demo {
    private static Logger LOG = Logger.getLogger(Demo.class);
    private static String Region_ID = "cn-hangzhou";
    private static String ACCESS_KEY_ID = "LTAIAfBoz0Wz5O6L";
    private static String ACCESS_SECRET = "WGlWEscL3u5rfFrokhYle4jFXsXBv9";
    private static String REGEX_PATTERN = "(mysql)(.*)(tar)";
    private static String SAVE_PATH = "/Users/personalc/project/binlogfiles";
    private static String DB_INSTANCE_ID = "rm-bp11gox03jgt2ullb";
    private static String BINLOG_ACTION_NAME = "DescribeBinlogFiles";

    public static void main(String[] args) {
        // 创建DefaultAcsClient实例并初始化
        DefaultProfile profile = DefaultProfile.getProfile(
                Region_ID,                     // 您的可用区ID
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
        int PageNumber;
        int PageRecordCount;
        try {
            binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
            totalRecordCount = binlogFilesResponse.getTotalRecordCount();
            System.out.println("totalRecordCount: " + totalRecordCount);
            List<BinLogFile> linkList = new ArrayList<>(totalRecordCount);
            PageNumber = binlogFilesResponse.getPageNumber();
            System.out.println("PageNumber: " + PageNumber);
            for (int i = 1; i < PageNumber; i++) {
                binlogFilesRequest.setPageNumber(i);
                binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
                PageRecordCount = binlogFilesResponse.getPageRecordCount();
                System.out.println("PageRecordCount: " + PageRecordCount);
                List<BinLogFile> items = binlogFilesResponse.getItems();
                linkList.addAll(items);
            }
            // TODO: 2018/5/3 记得增加检查数据文件完整性 
            linkList.stream().filter(binLogFile -> binLogFile.getHostInstanceID().equals("3691577")).forEach(binLogFile ->
            {
                try {
                    FileUtils.copyURLToFile(new URL(binLogFile.getDownloadLink()),
                            new File(SAVE_PATH + File.separator + binLogFile.getHostInstanceID() + "-" + getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从URL中解析下载的文件名
     *
     * @param link     下载连接
     * @param regexStr 文件名正则表达式
     * @return 文件名
     */
    private static String getFileNameFromUrl(String link, String regexStr) {
        String fileName = null;
        Pattern pattern = Pattern.compile(regexStr);
        Matcher matcher = pattern.matcher(link);
        if (matcher.find()) {
            fileName = matcher.group();
        } else {
            LOG.info("no fileName get from the link,please check the url or the regex pattern");
        }
        return fileName;
    }

}