package io.transwarp.sql.example;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by qls on 8/21/15.
 */
public class ExampleUtil {

    public static List<String> sqlList=new ArrayList<String>();


    public static void initSql() {
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s' and v1 = '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s' and v2 = '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s' and v3 = '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s' and v4 = '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key > '%s' and v1 > '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key < '%s' and v2 < '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key = '%s' and v2 = '%s' and v3='%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key > '%s' and v3 > '%s'");
        sqlList.add("Select * from jdbcRunnableExampleTest where key < '%s' and v4 < '%s'");
    }

    public static String createSQL(){
        String create_table_sql="create table jdbcRunnableExampleTest(key String, v1 double,v2 float,v3 bigint,v4 String)\n"+
                "stored by \n" +
                "'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n"+
                "tblproperties(\"hbase.table.name\"=\"jdbcRunnableExampleTest\")";
        System.out.println(create_table_sql);
        return create_table_sql;
    }

    public static String dropSQL(){
        String drop_table_sql="drop table if exists jdbcRunnableExampleTest";
        System.out.println(drop_table_sql);
        return drop_table_sql;
    }

    public static String insertSQL(int i){
        String insert_table_sql = "insert into jdbcRunnableExampleTest(key,v1,v2,v3,v4)values("+
                "'a"+String.format("%02d",i)+"',"+ (double)i+","+(float)i+","+i+",'d"+String.format("%02d",i)+"')";
        return insert_table_sql;
    }
}
