package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class GetDirectory {

    public void list(FileStatus fs){
        if(fs.isDirectory()){

        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String remotePath = "/user";

        Scanner sc = new Scanner(System.in);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        FileStatus fileStatus = fs.getFileStatus(new Path(remotePath));

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(remotePath), true);

        while(iterator.hasNext()){
            LocatedFileStatus next = iterator.next();
            System.out.println(next);
        }
    }
}
