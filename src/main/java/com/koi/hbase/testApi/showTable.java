package com.koi.hbase.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Scanner;

public class showTable {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");

        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
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

    public static void getAllRows(String name) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(name));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result: scanner) {
            System.out.println(result);
        }
        close();
    }

    public static void main(String[] args) {
        System.out.println("======================打印表信息======================");
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入表名称");
        String next = sc.next();
        try {
            getAllRows(next);
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
