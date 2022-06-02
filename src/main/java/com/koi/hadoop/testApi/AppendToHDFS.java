package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.*;
import java.net.URI;
import java.util.Scanner;

public class AppendToHDFS {

    public static void main(String[] args) throws IOException {
        String remotePath = "/user/hadoop/test/in.txt";
        String localPath = "F:\\课程之外\\大数据\\hadoop\\data\\hello.txt";
        String temp = "F:\\课程之外\\大数据\\hadoop\\data\\tmp.txt";
        Scanner sc = new Scanner(System.in);

        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create("hdfs://192.168.87.144:9000"), configuration, "root");
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("追击加到文件头还是文件尾，1.文件头，2.文件尾");
        while (true) {
            int opt = sc.nextInt();
            if (opt == 2) {
                FSDataOutputStream append = fs.append(new Path(remotePath));
                byte[] bytes = new byte[1024];
                File file = new File(localPath);
                FileInputStream fis = new FileInputStream(file);
                int len = 0;
                while ((len = fis.read(bytes)) != -1) {
                    append.write(bytes, 0, len);
                }
                fis.close();
                break;

            } else if (opt == 1) {
                //先将文件写到本地，再重新上传
                FSDataInputStream open = fs.open(new Path(remotePath));
                byte[] bytes = new byte[1024];
                File file = new File(localPath);

                FileOutputStream fos = new FileOutputStream(temp);
                FileInputStream fis = new FileInputStream(file);

                int len = 0;
                while ((len = fis.read(bytes)) != -1) {
                    fos.write(bytes, 0, len);
                }
                len = 0;
                while ((len = open.read(bytes)) != -1) {
                    fos.write(bytes, 0, len);
                }
                fos.close();
                fis.close();
                fs.copyFromLocalFile(false, true, new Path(temp), new Path(remotePath));
                break;
            } else {
                System.out.println("输入不规范，请重新输入");
            }
        }
        fs.close();
    }

}
