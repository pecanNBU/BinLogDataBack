

import com.aliyuncs.rds.model.v20140815.DescribeDBInstancesResponse;
import com.treefinance.binlog.util.DBInstanceUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        /*String str="ahbamysql-bin.0085.taraaa";
        String regex="(^mysql)(.*?)(tar$)";
        Pattern pattern=Pattern.compile(regex);
        Matcher m=pattern.matcher(str);
        System.out.println("hello world!");
        System.out.println(m.matches());
        if(m.find())
        {
            System.out.println(m.group(1));
        }*/
       /* try {
            TarUtil.decompress("/Users/personalc/project/binlogfiles/3691577-mysql-bin.000610.tar",new File("/Users/personalc/project/3691577-mysql-bin.000610"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        List<DescribeDBInstancesResponse.DBInstance> instances=DBInstanceUtil.getAllPrimaryDBInstance();

    }
}
