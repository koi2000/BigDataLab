package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;
import java.util.Scanner;

public class DownloadFromHDFS {

    public static void main(String[] args) throws IOException, InterruptedException {

        String remotePath = "/user/hadoop/test/in.txt";

        String localPath = "D:\\hadoop\\lab1\\";

        Scanner sc = new Scanner(System.in);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");
        String localName = "in.txt";
        File file = new File(localPath);
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            FileStatus fileStatus = fs.getFileStatus(new Path(remotePath));
            String name = fileStatus.getPath().getName();
            for (File f : files) {
                if (f.getName().equals(name)) {
                    Random r = new Random();
                    localName = r.nextInt()+".txt";
                }
            }
        }
        fs.copyToLocalFile(false, new Path(remotePath), new Path(localPath+localName));
    }
}
