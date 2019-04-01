package com.chaoyue.spark.project.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * HBase操作工具类，java工具类建议采用单例模式封装
 */
public class HBaseUtils {

    HBaseAdmin admin = null;
    Configuration configuration = null;

    private HBaseUtils(){

        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "hadoop000:2181");
        configuration.set("hbase.rootdir", "hdfs://hadoop000:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //单例模式
    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance(){
        if (null == instance){
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名得到Htable实例
     * @param tableName
     * @return
     */
    public HTable getTable(String tableName){
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 添加一条记录到表中
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param value
     */
    public void put(String tableName, String rowkey, String cf, String column, String value){
        HTable table = getTable(tableName);

        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

//        String tableName = "course_clickcount";
//        String rowkey = "20190223_1";
//        String cf = "info";
//        String column = "click_count";
//        String value = "33";
//
//        HBaseUtils.getInstance().put(tableName, rowkey, cf, column, value);

        HTable table = HBaseUtils.getInstance().getTable("course_clickcount");
        System.out.println(table.getName().getNameAsString());
    }
}
