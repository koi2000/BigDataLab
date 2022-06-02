package com.koi.hbase.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


public class ListTable {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        //configuration.set("hbase.rootdir","hdfs://192.168.87.144:9000/hbase");

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");
        //configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try{
            connection = ConnectionFactory.createConnection(configuration);
            //HBaseAdmin hBaseAdmin = new HBaseAdmin(connection);
            admin = connection.getAdmin();
            //admin = connection.getAdmin();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void listTables() throws IOException {
        init();
        HTableDescriptor[] hTableDescriptors = admin.listTables();
        for (HTableDescriptor hTableDescriptor: hTableDescriptors) {
            System.out.println(hTableDescriptor.getTableName());
        }
        close();
    }

    public static void main(String[] args) {
        System.out.println("======================打印所有表信息======================");
        try {
            listTables();
        } catch (IOException e) {
            System.out.println("出错了");
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
