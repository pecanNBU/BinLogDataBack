package com.treefinance.binlog.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author personalc
 */
public class TimeUtil {
    public static long dealDateFormat(String timeStr) {
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss'Z'");
        long timeStamp = 0;
        try {
            Date date = formatter.parse(timeStr);
            timeStamp = date.getTime();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return timeStamp;
    }
}
