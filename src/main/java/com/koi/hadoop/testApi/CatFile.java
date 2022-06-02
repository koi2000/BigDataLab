package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class CatFile {

    public static void main(String[] args) throws IOException, InterruptedException {
        String remotePath = "/user/hadoop/test/in.txt";

        String localPath = "D:\\hadoop\\lab1\\";

        Scanner sc = new Scanner(System.in);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        FSDataInputStream open = fs.open(new Path(remotePath));

        byte[] bytes = new byte[1024];
        char[]cs=new char[1024];
        int len;
        while((len = open.read(bytes))!= -1){
            System.out.println(new String(bytes,0,len));
        }
    }
}
