package com.koi.hbase.testApi;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Scanner;

public class CountTable {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;

    public static void init() {
        configuration = HBaseConfiguration.create();

        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("======================统计表的行数======================");
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入表名称");
        String tableName = sc.next();

        try {
            init();
            countTable(tableName);
        } catch (IOException e) {
            System.out.println("失败");
            e.printStackTrace();
        } finally {
            close();
        }
    }

    private static void countTable(String tableName) throws IOException {
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("表不存在");
            return;
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        ResultScanner results = table.getScanner(scan);
        int num = 0;
        for (Result res : results) {
            num++;
        }
        System.out.println(tableName + "表共有" + num + "行");

        table.close();
    }

    //关闭连接
    public static void close() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
