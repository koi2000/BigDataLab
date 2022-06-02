package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class CreateAndDelete {

    public static void main(String[] args) {
        String remotePath = "/user/hadoop/test/in.txt";

        String directory = "/user/hadoop/new";
        String fileName = "test.out";
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        try {
            if (!fs.exists(new Path(directory))) {
                System.out.println("目录不存在，以创建");
                fs.create(new Path(directory));
            } else {
                System.out.println("目录存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            FSDataOutputStream fsDataOutputStream = fs.create(new Path(directory + "/" + fileName));
            fsDataOutputStream.close();
            System.out.println("创建成功");
        } catch (IOException e) {
            System.out.println("创建失败");
            e.printStackTrace();
        }
        try {
            boolean delete = fs.delete(new Path(directory + "/" + fileName), true);
            if(delete){
                System.out.println("删除成功");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
