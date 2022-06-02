package com.koi.hbase.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Scanner;

public class general {

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

    public static void createTable() throws IOException {
        System.out.println("请输入待创建表名称");
        String tableName = sc.next();
        System.out.println("请输入字段数量");
        int num = sc.nextInt();
        String[] fields = new String[num];
        for (int i = 0; i < num; i++) {
            fields[i] = sc.next();
        }
        createTable(tableName, fields);
    }

    public static void addRecord() throws IOException {
        System.out.println("请输入表名称");
        String tableName = sc.next();
        System.out.println("请输入rowkey");
        String rowkey = sc.next();
        System.out.println("请输入字段数量");
        int num = sc.nextInt();
        System.out.println("请输如字段的列以及对应的value，列限定符使用:分割");
        String[] fields = new String[num];
        String[] values = new String[num];
        for (int i = 0; i < num; i++) {
            fields[i] = sc.next();
            values[i] = sc.next();
        }

        addRecord(tableName, rowkey, fields, values);
    }

    public static void scanColumn() throws IOException {
        System.out.println("请输入表名称");
        String tableName = sc.next();
        System.out.println("请输入列名");
        String column = sc.next();

        scanColumn(tableName, column);
    }

    public static void modifyData() throws IOException {
        System.out.println("请输入表名称");
        String tableName = sc.next();
        System.out.println("请输入rowkey");
        String rowkey = sc.next();
        System.out.println("请输入column");
        String column = sc.next();
        modifyData(tableName, rowkey, column);
    }

    public static void deleteRow() throws IOException {
        System.out.println("请输入表名称");
        String tableName = sc.next();
        System.out.println("请输入rowkey");
        String rowkey = sc.next();

        deleteRow(tableName, rowkey);
    }

    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            System.out.println("表已存在");
            admin.disableTable(name);
            admin.deleteTable(name);
            System.out.println("已删除");
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(name);
        for (String str : fields) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        System.out.println("表创建成功");
    }

    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (int i = 0; i < fields.length; i++) {
            Put put = new Put(row.getBytes());
            String[] cols = fields[i].split(":");
            if (cols.length == 1) {
                put.addColumn(cols[0].getBytes(), "".getBytes(), values[i].getBytes());
            } else {
                put.addColumn(cols[0].getBytes(), cols[1].getBytes(), values[i].getBytes());
            }

            table.put(put);
        }
        table.close();
    }

    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] cols = column.split(":");
        if (cols.length == 1) {
            scan.addFamily(column.getBytes());
        } else {
            scan.addColumn(cols[0].getBytes(), cols[1].getBytes());
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result res : scanner) {
            System.out.println(res);
        }
        table.close();
    }

    public static void modifyData(String tableName, String row, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        System.out.println("新的值为");
        String value = sc.next();
        Put put = new Put(row.getBytes());
        String[] cols = column.split(":");
        if (cols.length == 1) {
            put.addColumn(column.getBytes(), "".getBytes(), value.getBytes());
        } else {
            put.addColumn(cols[0].getBytes(), cols[1].getBytes(), value.getBytes());
        }
        table.put(put);
        table.close();
    }

    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(row.getBytes());
        table.delete(delete);
        table.close();
    }

    public static void main(String[] args) {
        System.out.println("======================综合实验======================");
        sc = new Scanner(System.in);
        //String next = sc.next();
        try {
            init();
            createTable();
            addRecord();
            scanColumn();
            modifyData();
            deleteRow();
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
