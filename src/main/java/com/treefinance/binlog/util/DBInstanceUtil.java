package com.treefinance.binlog.util;

import java.util.*;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigRequest;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstanceHAConfigResponse;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesRequest;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse;
import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse.DBInstance;

import org.apache.log4j.Logger;

public class DBInstanceUtil {
    private static Logger LOG = Logger.getLogger(DBInstance.class);
    private static Properties properties = FileUtil.getProperties();
    private static final int PAGE_SIZE = Integer.valueOf(properties.getProperty("PAGE_SIZE"));
    private static final String REGION_ID = properties.getProperty("REGION_ID");
    private static final String ACCESS_KEY_ID = properties.getProperty("ACCESS_KEY_ID");
    private static final String ACCESS_SECRET = properties.getProperty("ACCESS_SECRET");
    private static final DefaultProfile profile;
    private static IAcsClient client;

    static {
        profile = DefaultProfile.getProfile(
                REGION_ID,                     // 您的可用区ID
                ACCESS_KEY_ID,                  // 您的AccessKey ID
                ACCESS_SECRET);                // 您的AccessKey Secret
    }

    /**
     * 云平台连接设置
     *
     * @return IAcsClient实例
     */
    private static IAcsClient createConnection() {
        if (null == client) {
            client = new DefaultAcsClient(profile);
        }
        return client;

    }

    /**
     * 获取所有Mysql数据库实例（DBInstance）
     *
     * @return 返回所有的实例
     */
    public static List<DBInstance> getAllPrimaryDBInstance() {
        IAcsClient client = createConnection();
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest();
        DescribeDBInstancesResponse dbInstancesResponse;
        List<DBInstance> dbInstances = null;
        dbInstancesRequest.setDBInstanceType("Primary");
        try {
            dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
            int totalInstance = dbInstancesResponse.getTotalRecordCount();
            dbInstances = new ArrayList<>(totalInstance);
            int PageCount = 0;
            if (totalInstance > 0) {
                PageCount = (int) Math.ceil(totalInstance / PAGE_SIZE);
            }
            LOG.info("pageCount: " + PageCount);
            for (int i = 1; i <= PageCount; i++) {
                dbInstancesRequest.setPageNumber(i);
                dbInstancesResponse = client.getAcsResponse(dbInstancesRequest, profile);
                List<DBInstance> dbInstanceList = dbInstancesResponse.getItems();
                for (DBInstance dbInstance : dbInstanceList) {
                    System.out.println(dbInstance.getDBInstanceId());
                }
                System.out.println("****************" + dbInstanceList.size());
                dbInstances.addAll(dbInstanceList);
            }
        } catch (ClientException e) {
            e.printStackTrace();
        }
        return dbInstances;
    }

    /**
     * 获取实例的备份实例编号
     *
     * @param dbInstance 某个实例
     * @return 备份实例编号
     */
    public static String getBackInstanceId(DBInstance dbInstance) {
        IAcsClient client = DBInstanceUtil.createConnection();
        DescribeDBInstanceHAConfigRequest haConfigRequest = new DescribeDBInstanceHAConfigRequest();
        String instanceId = dbInstance.getDBInstanceId();
        haConfigRequest.setActionName("DescribeDBInstanceHAConfig");
        haConfigRequest.setDBInstanceId(instanceId);
        String backInstanceId = null;
        try {
            DescribeDBInstanceHAConfigResponse haConfigResponse = client.getAcsResponse(haConfigRequest, DBInstanceUtil.getProfile());
            List<DescribeDBInstanceHAConfigResponse.NodeInfo> hostInstanceInfos = haConfigResponse.getHostInstanceInfos();
            for (DescribeDBInstanceHAConfigResponse.NodeInfo hostInstanceInfo : hostInstanceInfos) {
                if (hostInstanceInfo.getNodeType().equals("Slave")) {
                    backInstanceId = hostInstanceInfo.getNodeId();
                    System.out.println(backInstanceId);
                }
            }

        } catch (ClientException e) {
            e.printStackTrace();
        }

        return backInstanceId;
    }

    private static DefaultProfile getProfile() {
        return profile;
    }
}

