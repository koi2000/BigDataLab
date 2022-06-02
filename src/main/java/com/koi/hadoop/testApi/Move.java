package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class Move {

    public static void main(String[] args) throws IOException {
        String remotePath = "/user/hadoop/test/in.txt";

        String newPath = "/user/hadoop/in.txt";

        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        fs.rename(new Path(remotePath),new Path(newPath));
        fs.close();
    }

}
