package com.panli.data.sources.customs.mysql;

import com.panli.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc 自定义 Mysql Source
 */
public class SourceFromMySQL extends RichSourceFunction<Student> {

    private PreparedStatement pstmt;
    private Connection conn;

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next()) {
            Student student = new Student(
                    resultSet.getInt("id"),
                    resultSet.getString("name").trim(),
                    resultSet.getString("password").trim(),
                    resultSet.getInt("age"));
            ctx.collect(student);
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConn();
        String sql = "select * from tb_student";
        pstmt = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭释放链接
        if (conn != null) {
            conn.close();
        }
        if (pstmt != null) {
            pstmt.close();
        }
    }

    private static Connection getConn() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/streaming_flink?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
