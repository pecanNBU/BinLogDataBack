package com.treefinance.binlog.util;

public class LogUtil {

    public static void sleep(int mills){
        try{
            Thread.sleep(mills);
        }catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

}