package com.treefinance.binlog.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;

import org.apache.log4j.Logger;

public class DBInstanceUtil {
    private static Logger LOG = Logger.getLogger(DBInstance.class);
    private static Properties properties = PropertiesUtil.getProperties();
    private static final String Engine = "";
    private static final String DBInstanceType = "MySQL";
    private static final String InstanceNetworkType = "";
    private static final String ConnectionMode = "";
    private static final String Tags = "";
    private static final int PageSize = 30;
    private static final String PageNumber = "";
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final DefaultProfile profile;

    static {
        profile = DefaultProfile.getProfile(
                REGION_ID,                     // 您的可用区ID
                ACCESS_KEY_ID,                  // 您的AccessKey ID
                ACCESS_SECRET);                // 您的AccessKey Secret
    }

    public static void main(String[] args) {
        List<DBInstance> dbInstances = getAllDBInstance();
        for (DBInstance dbInstance : dbInstances) {
            String instanceId=dbInstance.getDBInstanceId();
            String masterInstanceId=dbInstance.getMasterInstanceId();
            String instanceType= String.valueOf(dbInstance.getDBInstanceType());
            String bakInstanceId=dbInstance.getVpcId();
            System.out.println(bakInstanceId);
            System.out.println(instanceId+"===="+masterInstanceId+"==="+instanceType);
        }

    }

    private static IAcsClient createConnection() {
        return new DefaultAcsClient(profile);
    }

    /**
     * 获取所有Mysql数据库实例（DBInstance）
     *
     * @return 返回所有的实例
     */
    private static List<DBInstance> getAllDBInstance() {
        IAcsClient client = createConnection();
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
        DescribeDBInstancesResponse dbInstancesResponse;
        List<DBInstance> dbInstances = null;
        try {
            dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
            int totalInstance = dbInstancesResponse.getTotalRecordCount();
            dbInstances = new ArrayList<>(totalInstance);
            int PageCount = 0;
            if (totalInstance > 0) {
                PageCount = (int) Math.ceil(totalInstance / PageSize);
            }
            LOG.info("pageCount: " + PageCount);
            for (int i = 1; i <= PageCount; i++) {
                dbInstancesRequest.setPageNumber(i);
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                dbInstances.addAll(dbInstanceList);
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return dbInstances;
    }
}

