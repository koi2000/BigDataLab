package com.koi.databases;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class hbaseTest {

    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    public static Scanner sc;

    public static void init() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void scanColumn() throws IOException {
        System.out.println("请输入表名称");
        String tableName = sc.next();
        System.out.println("请输入rowkey");
        String rowKey = sc.next();
        System.out.println("请输入列名");
        String column = sc.next();

        scanColumn(tableName, rowKey, column);
    }

    public static void scanColumn(String tableName,String rowKey, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        Get get = new Get(rowKey.getBytes());


        String[] cols = column.split(":");

        get.addColumn(cols[0].getBytes(),cols[1].getBytes());
        Result result = table.get(get);
        for (Cell cell : result.rawCells()){
            String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(colName+" "+value);
        }
//        if (cols.length == 1) {
//            scan.addFamily(column.getBytes());
//        } else {
//            scan.addColumn(cols[0].getBytes(), cols[1].getBytes());
//        }
//        ResultScanner scanner = table.getScanner(scan);
//        for (Result res : scanner) {
//            if(Arrays.toString(res.getRow()).equals(rowKey)){
//                System.out.println(res);
//            }
//        }
        table.close();
    }

    public static void main(String[] args) {
        System.out.println("======================综合实验======================");
        sc = new Scanner(System.in);
        //String next = sc.next();
        try {
            init();
            //addRecord();
            scanColumn();
        } catch (IOException e) {
            System.out.println("出错了");
            e.printStackTrace();
        }
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
