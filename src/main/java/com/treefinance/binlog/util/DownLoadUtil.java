package com.treefinance.binlog.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

@Deprecated
public class DownLoadUtil {
    public static void downLoadFile(String src, String des) {
        try {
            File file = new File(des);
            HttpURLConnection connection = (HttpURLConnection) new URL(src).openConnection();
            connection.setRequestMethod("GET");
            long sum = 0;
            if (file.exists()) {
                sum = file.length();
                // 设置断点续传的开始位置
                connection.setRequestProperty("Range", "bytes=" + sum + "-");
            }
            int code = connection.getResponseCode();
            System.out.println("code = " + code);
            if (code == 200 || code == 206) {
                int contentLength = connection.getContentLength();
                System.out.println("contentLength = " + contentLength);
                contentLength += sum;
                InputStream is = null;
                try {
                    is = connection.getInputStream();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                /*
                 *
                 * 创建一个向具有指定 name 的文件中写入数据的输出文件流。
                 * true表示当文件在下载过程中出现中断，
                 * 当再次链接网络时，将会从断点处追加。
                 *
                 * */
                FileOutputStream fos = new FileOutputStream(file, true);

                byte[] buffer = new byte[1024];
                int length;
                long startTime = System.currentTimeMillis();
                while ((length = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, length);
                    sum += length;
                    float percent = sum * 100.0f / contentLength;
                    System.out.print("\r[");
                    int p = (int) percent / 2;
                    /*
                     * 实现进度条
                     * */
                    for (int i = 0; i < 50; i++) {
                        if (i < p) {
                            System.out.print('=');
                        } else if (i == p) {
                            System.out.print('>');
                        } else {
                            System.out.print(' ');
                        }
                    }
                    System.out.print(']');
                    System.out.printf("\t%.2f%%", percent);
                    long speed = sum * 1000 / (System.currentTimeMillis() - startTime);
                    if (speed > (1 << 20)) {
                        System.out.printf("\t%d MB/s", speed >> 20);
                    } else if (speed > (1 << 10)) {
                        System.out.printf("\t%d KB/s", speed >> 10);
                    } else {
                        System.out.printf("\t%d B/s", speed);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
