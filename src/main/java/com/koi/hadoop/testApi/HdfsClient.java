package com.koi.hadoop.testApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.87.144:9000"), configuration,"root");

        // 2 创建目录
        //fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        //查看目录
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/user/hadoop"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            System.out.println(locatedFileStatusRemoteIterator);
            locatedFileStatusRemoteIterator.next();
        }
        // 3 关闭资源
        fs.close();
    }


}
