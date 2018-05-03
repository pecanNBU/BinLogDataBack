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
    private static String SAVE_PAHT = "/Users/personalc/project/binlogfiles";
    private static int HTTP_CONNECTION_TIMEOUT = 10 * 1000;
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
        String downloadLink;
        String hostInstanceID;
        String linkExpiredTime;
        int totalRecordCount;
        int PageNumber;
        int PageRecordCount;
        try {
            binlogFilesResponse = client.getAcsResponse(binlogFilesRequest, profile);
            totalRecordCount = binlogFilesResponse.getTotalRecordCount();
            System.out.println("totalRecordCount: " + totalRecordCount);

            List<BinLogFile> linkList = new ArrayList<BinLogFile>(totalRecordCount);

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

            String fileName;
            linkList.stream().filter(binLogFile -> binLogFile.getHostInstanceID().equals("3691577")).forEach(binLogFile ->
            {
                try {
                    FileUtils.copyURLToFile(new URL(binLogFile.getDownloadLink()),
                            new File(SAVE_PAHT + File.separator + binLogFile.getHostInstanceID() + "-" + getFileNameFromUrl(binLogFile.getDownloadLink(), REGEX_PATTERN)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            /*int size = linkList.size();
            for (int j = 0; j < size; j++) {
                System.out.println("当前文件数：" + j);
                BinLogFile binLogFile = linkList.get(j);
                downloadLink = binLogFile.getDownloadLink();
                System.out.println("downloadLink: " + downloadLink);
                hostInstanceID = binLogFile.getHostInstanceID();
                System.out.println("hostInstanceID: " + hostInstanceID);
                linkExpiredTime = binLogFile.getLinkExpiredTime();
                System.out.println("linkExpiredTime: " + linkExpiredTime);
                fileName = hostInstanceID + "-" + getFileNameFromUrl(downloadLink, REGEX_PATTERN);
                System.out.println("文件名：" + fileName);
                try {
                    //downLoadFromUrl(downloadLink, fileName, SAVE_PAHT);
                    File file = new File(SAVE_PAHT + File.separator + fileName);
                    FileUtils.copyURLToFile(new URL(downloadLink), file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }*/
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 从网络Url中下载文件
     *
     * @param urlStr
     * @param fileName
     * @param savePath
     * @throws IOException
     */
    public static void downLoadFromUrl(String urlStr, String fileName, String savePath) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        //设置超时间为3秒
        conn.setConnectTimeout(HTTP_CONNECTION_TIMEOUT);
        //防止屏蔽程序抓取而返回403错误
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.110 Safari/537.36");

        //得到输入流
        InputStream inputStream = conn.getInputStream();
        //获取自己数组
        byte[] getData = readInputStream(inputStream);

        //文件保存位置
        File saveDir = new File(savePath);
        if (!saveDir.exists()) {
            saveDir.mkdir();
        }
        File file = new File(saveDir + File.separator + fileName);
        FileOutputStream fos = new FileOutputStream(file);
        fos.write(getData);
        if (fos != null) {
            fos.close();
        }
        if (inputStream != null) {
            inputStream.close();
        }
        System.out.println("info:" + url + " download success");

    }

    /**
     * 从输入流中获取字节数组
     *
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static byte[] readInputStream(InputStream inputStream) throws IOException {
        byte[] buffer = new byte[1024];
        int len;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while ((len = inputStream.read(buffer)) != -1) {
            bos.write(buffer, 0, len);
        }
        bos.close();
        return bos.toByteArray();
    }

    /**
     * 从URL中解析下载的文件名
     *
     * @param link     下载连接
     * @param regexStr 文件名正则表达式
     * @return 文件名
     */
    public static String getFileNameFromUrl(String link, String regexStr) {
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