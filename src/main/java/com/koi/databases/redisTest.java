package com.koi.databases;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Objects;
import java.util.Scanner;

public class redisTest {

    public static void main(String[] args) {
        // 获取连接池配置对象
        JedisPoolConfig config = new JedisPoolConfig();
        // 设置最大连接数
        config.setMaxTotal(30);
        // 设置最大的空闲连接数
        config.setMaxIdle(10);

        // 获得连接池: JedisPool jedisPool = new JedisPool(poolConfig,host,port);
        JedisPool jedisPool = new JedisPool(config,"152.136.26.27",6379);
        // 获得核心对象：jedis
        Jedis jedis = null;
        try{
            // 通过连接池来获得连接
            jedis = jedisPool.getResource();
            //设置密码
            jedis.auth("123456");
            // 设置数据
            jedis.set("name","张三");
            Scanner sc = new Scanner(System.in);
            System.out.println("请输入用户名");
            String name = sc.next();
            System.out.println("请输入英语成绩");
            String english = sc.next();
            System.out.println("请输入数学成绩");
            String math = sc.next();
            System.out.println("请输入计算机成绩");
            String computer = sc.next();
            HashMap<String, String>stu = new HashMap<>();
            stu.put("english",english);
            stu.put("math",math);
            stu.put("computer",computer);
            jedis.hset(name,stu);

            String score = jedis.hget(name, "english");
            System.out.println(name+"的英语成绩为"+score);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            // 释放资源
            if(jedis != null){
                jedis.close();
            }
            // 释放连接池
            if(jedisPool != null){
                jedisPool.close();
            }
        }
    }
}
