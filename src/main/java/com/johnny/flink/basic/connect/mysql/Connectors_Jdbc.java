package com.johnny.flink.basic.connect.mysql;

import com.johnny.flink.model.Student;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/2/25 19:36
 */
public class Connectors_Jdbc {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(new Student(1, "Johnny", 18))
                        .addSink(JdbcSink.sink(
                                "INSERT INTO t_student (id, name, age) VALUES (null, ?, ?)",
                                (statement, student) -> {
                                    statement.setString(1, student.getName());
                                    statement.setInt(2, student.getAge());
                                }, new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl("jdbc:mysql://node01:3306/test")
                                        .withDriverName("com.mysql.cj.jdbc.Driver")
                                        .withUsername("root")
                                        .withPassword("root")
                                        .build()
                        ));

        env.execute();
    }

}
