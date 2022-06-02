package com.koi.databases;

import java.sql.*;
import java.util.Scanner;

public class mysqlTest {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://192.168.87.144:33306/bigdata";

    // 数据库的用户名与密码，需要根据自己的设置
    static final String USER = "root";
    static final String PASS = "242200";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        Scanner sc = new Scanner(System.in);
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);

            // 打开链接
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            // 执行查询
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            System.out.println("请输入姓名");
            String name = sc.next();
            System.out.println("请输入英语成绩");
            int english = sc.nextInt();
            System.out.println("请输入数学成绩");
            int math = sc.nextInt();
            System.out.println("请输入计算机成绩");
            int computer = sc.nextInt();

            sql = "insert into student values(?,?,?,?);";

            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            preparedStatement.setString(1,name);
            preparedStatement.setInt(2,english);
            preparedStatement.setInt(3,math);
            preparedStatement.setInt(4,computer);
            boolean execute = preparedStatement.execute();
            if(execute){
                System.out.println("添加成功");
            }else {
                System.out.println("添加失败");
            }

            String query = "select s_english from student where s_name = ?";
            PreparedStatement preparedStatement1 = conn.prepareStatement(query);
            preparedStatement1.setString(1,"Scofield");

            ResultSet resultSet = preparedStatement1.executeQuery();
            if(resultSet.next()){
                //Your code
                System.out.println(resultSet.getInt(1));
            }
            // 完成后关闭
            resultSet.close();
            stmt.close();
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
            }catch(SQLException se2){
            }// 什么都不做
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }
}
