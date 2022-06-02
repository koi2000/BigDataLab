package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class GetFileStatus {

    public static void main(String[] args) throws IOException, InterruptedException {
        String remotePath = "/user/hadoop/test/in.txt";

        Scanner sc = new Scanner(System.in);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        FileStatus fileStatus = fs.getFileStatus(new Path(remotePath));

        System.out.println("========" + fileStatus.getPath() + "=========");
        System.out.println(fileStatus.getPermission());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getLen());
        System.out.println(fileStatus.getModificationTime());
        System.out.println(fileStatus.getReplication());
        System.out.println(fileStatus.getBlockSize());
        System.out.println(fileStatus.getPath().getName());

    }

}
