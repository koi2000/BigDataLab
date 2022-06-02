package com.koi.hbase.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import javax.xml.bind.SchemaOutputResolver;
import java.io.IOException;
import java.util.Scanner;

public class AddCol {

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

    public static void main(String[] args) throws IOException {
        System.out.println("======================向已经创建的表添加或者删除列族或列======================");
        sc = new Scanner(System.in);
        System.out.println("请输入您想要进行的操作，1。添加，2.删除");
        init();
        while (true) {
            int opt = sc.nextInt();

            if (opt == 1) {
                System.out.println("请输入表名");
                String tableName = sc.next();
                System.out.println("对列族操作还是对列操作，1.列族，2.列");
                int opt2 = sc.nextInt();
                if (opt2 == 1) {
                    addColGroup(tableName);
                }
                if (opt2 == 2) {
                    addCol(tableName);
                }

                break;
            } else if (opt == 2) {
                System.out.println("请输入表名");
                String tableName = sc.next();
                System.out.println("对列族操作还是对列操作，1.列族，2.列");
                int opt2 = sc.nextInt();
                if (opt2 == 1) {
                    removeColGroup(tableName);
                }
                if (opt2 == 2) {
                    removeCol(tableName);
                }

                break;
            } else {
                System.out.println("输入数据有误，请重新输入");
            }
        }
        close();
    }

    private static void removeCol(String tableName) throws IOException {
        if(!admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表不存在");
            return;
        }

        System.out.println("请输入列族名");
        String colFamily = sc.next();

        System.out.println("请输入列名");
        String col = sc.next();

        System.out.println("请输入rowKey");
        String rowKey = sc.next();

        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(colFamily.getBytes(), col.getBytes());
        table.delete(delete);
        table.close();
    }

    private static void removeColGroup(String tableName) throws IOException {
        if(!admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表不存在");
            return;
        }

        System.out.println("请输入列族名");
        String col = sc.next();
        admin.deleteColumnFamily(TableName.valueOf(tableName),col.getBytes());

    }

    private static void addCol(String tableName) throws IOException {
        if(!admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表不存在");
            return;
        }

        System.out.println("请输入列族名");
        String colFamily = sc.next();

        System.out.println("请输入列名");
        String col = sc.next();

        System.out.println("请输入value");
        String val = sc.next();

        System.out.println("请输入rowKey");
        String rowKey = sc.next();

//        admin.modifyColumnFamily();
//        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
//        admin.addColumnFamily(TableName.valueOf(tableName),hColumnDescriptor);

        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(), col.getBytes(), val.getBytes());
        table.put(put);
        table.close();
    }

    private static void addColGroup(String tableName) throws IOException {
        if(!admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表不存在");
            return;
        }

        System.out.println("请输入列族名");
        String col = sc.next();
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
        admin.addColumnFamily(TableName.valueOf(tableName),hColumnDescriptor);
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
