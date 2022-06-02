package com.koi.databases;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.Scanner;

public class mongodbTest {

    public static void main(String args[]) {

        try {
            Scanner sc = new Scanner(System.in);
            MongoClient mongoClient = new MongoClient("192.168.87.144", 27017);

            MongoDatabase mgdb = mongoClient.getDatabase("test");

            MongoCollection<Document> collection = mgdb.getCollection("student");

            System.out.println("请输入用户名");
            String name = sc.next();
            System.out.println("请输入英语成绩");
            String english = sc.next();
            System.out.println("请输入数学成绩");
            String math = sc.next();
            System.out.println("请输入计算机成绩");
            String computer = sc.next();
            HashMap<String, String> stu = new HashMap<>();
            stu.put("english",english);
            stu.put("math",math);
            stu.put("computer",computer);

            Document document = new Document("_id", 2555).append("name", name)
                    .append("score",stu);

            collection.insertOne(document);

            //多条件查询   and 与  or 或
            Bson peopleName = Filters.eq("name", "scofield");
            Bson score = Filters.eq("score",1);

            final FindIterable<Document> peoples = collection.find(peopleName).projection(score);
            for (Document pe : peoples) {
                System.out.println(pe.toJson());
            }

        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
