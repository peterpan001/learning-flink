package com.panli.data.sinks.customs.kafka;

import com.panli.domain.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author lipan
 * @date 2021-01-12
 * @desc 自定义 Sink
 */
public class SinkToMySQL extends RichSinkFunction<Student> {

    private PreparedStatement pstmt;
    private Connection conn;

    @Override
    public void invoke(Student value, Context ctx) throws Exception {
        pstmt.setInt(1, value.getId());
        pstmt.setString(2, value.getName());
        pstmt.setString(3, value.getPwd());
        pstmt.setInt(4, value.getAge());
        pstmt.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConn();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        pstmt = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
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
