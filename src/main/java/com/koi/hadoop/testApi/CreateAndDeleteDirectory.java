package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

public class CreateAndDeleteDirectory {

    public static void main(String[] args) throws IOException, URISyntaxException {
        String remotePath = "/user/hadoop/test/in.txt";
        Scanner sc = new Scanner(System.in);
        String directory = "/user/hadoop/ques7";
        String fileName = "test.out";
        Configuration configuration = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://192.168.87.144:9000"), configuration, "root");
        } catch (InterruptedException e) {
            System.out.println("获取文件系统失败");
            return;
        }

        //检测目录是否存在
        if (!fs.exists(new Path(directory))) {
            System.out.println("目录不存在，正在自动创建");
            try {
                fs.mkdirs(new Path(directory));
            } catch (IOException e) {
                System.out.println("出现异常");
                return;
            }
            System.out.println("已创建完毕");
        }

        FileStatus fileStatus = fs.getFileStatus(new Path(directory));
        if (!fileStatus.isDirectory()) {
            System.out.println("非目录，正在删除");
            try {
                fs.delete(new Path(directory), true);
            } catch (IOException e) {
                System.out.println("删除失败");
            }
            System.out.println("已删除");
            return;
        }

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(directory), true);
        int cnt = 0;
        //查看当前目录下文件个数
        if (iterator.hasNext()) {
            cnt++;
            iterator.next();
        }
        //如果为0，说明目录为空
        if (cnt > 0) {
            System.out.println("当前目录不为空");
            return;
        }

        System.out.println("是否删除当前目录，1.删除，2.不删除");
        while (true) {
            int opt = sc.nextInt();
            if (opt == 1) {
                try {
                    fs.delete(new Path(directory), true);
                } catch (IOException e) {
                    System.out.println("删除失败");
                }
            } else if (opt == 2) {
                return;
            }
        }
    }

}
