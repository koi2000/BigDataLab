package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class UploadToHDFS {

    private boolean isExist = false;

    public static void overwrite(FileSystem fs, String localPath, String remotePath) throws IOException {
        fs.copyFromLocalFile(false, true, new Path(localPath), new Path(remotePath));
    }

    public static void append(FileSystem fs, String localPath, String remotePath) throws IOException {
        //fs.copyFromLocalFile(false, false, new Path(localPath), new Path(remotePath));
        FSDataOutputStream append = fs.append(new Path(remotePath));
        File file = new File(localPath);
        FileInputStream fis = new FileInputStream(localPath);

        byte[] bytes = new byte[1024];
        int len = 0;
        while ((len = fis.read(bytes)) != -1) {
            append.write(bytes, 0, len);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        String localPath = "F:\\课程之外\\大数据\\hadoop\\笔记（word版本）\\phone_data.txt";

        String remotePath = "/user/hadoop/test/in.txt";

        Scanner sc = new Scanner(System.in);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");

        if (!new File(localPath).exists()) {
            System.out.println("文件不存在");
            return;
        }

        if (fs.exists(new Path(remotePath))) {
            System.out.println("当前文件已存在，需要追加还是覆盖 1.追加 2.重写");
            while (true) {
                int opt = sc.nextInt();
                if (opt == 1) {
                    append(fs, localPath, remotePath);
                    break;
                } else if (opt == 2) {
                    overwrite(fs, localPath, remotePath);
                    break;
                } else {
                    System.out.println("命令不规范，请重新输入");
                }
            }
        } else {
            overwrite(fs, localPath, remotePath);
        }
    }
}
