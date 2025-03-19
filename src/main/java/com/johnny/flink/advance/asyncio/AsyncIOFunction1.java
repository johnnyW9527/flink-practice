package com.johnny.flink.advance.asyncio;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.List;

/**
 * <b>Java-vertx中提供的异步client实现异步IO</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 10:50
 */
public class AsyncIOFunction1 extends RichAsyncFunction<CategoryInfo, CategoryInfo> {

    private transient SQLClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        JsonObject mysqlConfig = new JsonObject();
        mysqlConfig
                .put("driver_class", "com.mysql.jdbc.Driver")
                .put("url", "jdbc:mysql://10.21.16.154:3306/dsg25_wanliang?serverTimezone=GMT%2B8&characterEncoding=utf8&useUnicode=true&useSSL=false")
                .put("user", "root")
                .put("password", "asiainfo")
                .put("max_pool_size", 20);
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(10);
        vertxOptions.setWorkerPoolSize(20);
        Vertx vertx = Vertx.vertx(vertxOptions);
        // 根据上面配置参数获取异步请求客户端
        client = JDBCClient.createNonShared(vertx, mysqlConfig);
    }


    /**
     * 异步调用方法，用于查询类别信息
     *
     * @param categoryInfo 包含类别ID的类别信息对象，用于指定查询的类别
     * @param resultFuture 用于处理查询结果的Future对象
     * @throws Exception 如果查询过程中发生错误，则抛出异常
     */
    @Override
    public void asyncInvoke(CategoryInfo categoryInfo, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        // 请求数据库连接，用于执行查询操作
        client.getConnection(new Handler<AsyncResult<SQLConnection>>() {
            @Override
            public void handle(AsyncResult<SQLConnection> sqlConnectionAsyncResult) {
                // 如果获取连接失败，则直接返回，不进行后续操作
                if (sqlConnectionAsyncResult.failed()) {
                    return;
                }
                // 获取数据库连接对象
                SQLConnection connection = sqlConnectionAsyncResult.result();
                // 执行SQL查询语句，查询与给定类别ID匹配的类别信息
                connection.query("select id,name from t_category where id = "+ categoryInfo.getId(), new Handler<AsyncResult<io.vertx.ext.sql.ResultSet>>() {
                    @Override
                    public void handle(AsyncResult<ResultSet> resultSetAsyncResult) {
                        // 如果查询成功，则处理查询结果
                        if (resultSetAsyncResult.succeeded()) {
                            // 获取查询结果的行列表
                            List<JsonObject> rows =
                                    resultSetAsyncResult.result().getRows();
                            // 遍历每一行结果，创建类别信息对象，并完成Future对象
                            for (JsonObject row : rows) {
                                CategoryInfo category = new CategoryInfo(row.getInteger("id"), row.getString("name"));
                                resultFuture.complete(Collections.singletonList(category));
                            }
                        }
                    }
                });
            }
        });
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public void timeout(CategoryInfo input, ResultFuture<CategoryInfo> resultFuture) throws Exception {
        System.out.println("超时了");
        input.setName("超时了");
        resultFuture.complete(Collections.singletonList(input));
    }
}
