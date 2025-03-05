package com.johnny.flink.basic.source;

import com.johnny.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * <b>自定义Mysql源source</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 10:43
 */
public class SourceCustomerMySql {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Student> sqlDs = env.addSource(new MySqlSource()).setParallelism(1);

        sqlDs.print();

        env.execute();
    }

    static class MySqlSource extends RichParallelSourceFunction<Student> {

        private Connection conn = null;
        private PreparedStatement stmt = null;
        private boolean isRunning = true;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root");
            String sql = "select id , name, age from t_student";
            stmt = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Student> sourceContext) throws Exception {
            while (isRunning) {
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    int age = resultSet.getInt("age");
                    sourceContext.collect(new Student(id, name, age));
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) {
                conn.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

}
