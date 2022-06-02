package com.koi.hbase.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;

public class DeleteAll {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");

        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }


    public static void clearTable(String tableName) throws IOException {
        if(!admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.truncateTable(TableName.valueOf(tableName),true);
        System.out.println("清空成功");
    }

    public static void main(String[] args) {
        System.out.println("======================清空指定表======================");
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入表名称");
        String tableName = sc.next();
        try {
            init();
            clearTable(tableName);
        } catch (IOException e) {
            System.out.println("清空失败");
            e.printStackTrace();
        }
    }

    //关闭连接
    public static void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
