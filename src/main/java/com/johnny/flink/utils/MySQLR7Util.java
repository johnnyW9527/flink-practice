package com.johnny.flink.utils;

import com.google.common.base.CaseFormat;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/19 10:00
 */
public class MySQLR7Util {

    static Logger logger = LoggerFactory.getLogger(MySQLR7Util.class);

    private static volatile JdbcTemplate jdbcTemplate;

    /**
     * 单例获取MySql数据库的连接
     * @return  JdbcTemplate
     */
    public static JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            synchronized (MySQLR7Util.class) {
                if (jdbcTemplate == null) {
                    try {
                        HikariDataSource ds = new HikariDataSource();
                        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
                        ds.setJdbcUrl(ModelUtil.getConfigValue("mysql.r7.yishou.url"));
                        ds.setUsername(ModelUtil.getConfigValue("mysql.r7.username"));
                        ds.setPassword(ModelUtil.getConfigValue("mysql.r7.password"));
                        ds.setMaximumPoolSize(50);
                        ds.setMinimumIdle(5);
                        jdbcTemplate = new JdbcTemplate(ds);
                        logger.info(
                                "##### 使用HikariPool连接池初始化JdbcTemplate成功，使用的URL为：{} , 其中最大连接大小为：{} , 最小连接大小为：{} ;",
                                ds.getJdbcUrl(),
                                ds.getMaximumPoolSize(),
                                ds.getMinimumIdle()
                        );
                    } catch (Exception e) {
                        throw new RuntimeException("创建 MySQL R7 数据库连接失败");
                    }
                }
            }
        }
        return jdbcTemplate;
    }

    /**
     * 处理特殊字符
     * 主要用于处理字符串中的特殊字符，以避免在某些上下文中（如数据库查询）引起问题
     *
     * @param data 待处理的字符串
     * @return 处理后的字符串
     */
    public static String disposeSpecialCharacter(String data) {
        // 处理其中的单引号
        data = data.replace("'", "''");
        // 返回结果
        return data;
    }


    /**
     * 根据主键查询列表
     * 该方法用于从指定表中，根据主键值查询指定字段的记录，并将结果映射为指定类的列表
     *
     * @param tableName 数据表名，用于指定查询的数据表
     * @param primaryKey 主键名，用于指定查询的条件字段
     * @param primaryValue 主键值，用于指定查询的条件值
     * @param clz 泛型类的Class对象，用于指定结果集映射的类类型
     * @param fields 字段列表，用于指定查询的字段
     * @return 返回一个泛型列表，列表中的元素为根据查询结果映射的指定类的实例
     */
    public static <T> List<T> queryListByKey(String tableName, String primaryKey, String primaryValue, Class<T> clz, String... fields) {

        // 拼接SQL
        // 根据传入的表名、主键、主键值和字段列表，动态生成SQL查询语句
        // 使用Stream API来拼接字段列表，提高代码的可读性和效率
        String sql = " select " +
                Arrays.stream(fields).map(String::valueOf).collect(Collectors.joining(",")) +
                " from " + tableName +
                " where " + primaryKey + " = '" + disposeSpecialCharacter(primaryValue) + "'";

        // 执行SQL并返回结果
        // 使用queryList方法执行拼接好的SQL查询语句，并将结果映射为指定类的列表
        return queryList(sql, clz);
    }


    /**
     * 查询 MySQL R7 数据库中的数据，并将结果封装成指定类型的列表
     *
     * @param sql 查询的SQL语句
     * @param clz 结果集中的对象类型
     * @param <T> 泛型参数，表示结果集中的对象类型
     * @return 包含查询结果的列表，列表中的每个元素都是clz指定类型的实例
     *
     * 此方法的工作原理：
     * 1. 使用提供的SQL查询语句从数据库中获取数据，数据以键值对的列表形式返回
     * 2. 遍历这个列表，对于每个键值对（代表查询结果的一行），通过反射创建一个指定类型的对象
     * 3. 使用BeanUtils.setProperty方法将每个键值对的值设置到新创建的对象中，键是列名，值是列的值
     * 4. 将填充了数据的对象添加到结果列表中
     * 5. 如果在查询或数据封装过程中发生任何异常，此方法将抛出RuntimeException，并包含详细的错误信息
     */
    public static <T> List<T> queryList(String sql, Class<T> clz) {
        try {
            // 执行SQL查询，获取结果列表，每个结果以键值对的形式表示
            List<Map<String, Object>> mapList = MySQLR7Util.getJdbcTemplate().queryForList(sql);
            List<T> resultList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                Set<String> keys = map.keySet();
                // 当返回的结果中存在数据，通过反射将数据封装成样例类对象
                T result = clz.newInstance();
                for (String key : keys) {
                    // 使用Apache Commons BeanUtils工具库将键值对的值设置到反射创建的对象中
                    BeanUtils.setProperty(
                            result,
                            key,
                            map.get(key)
                    );
                }
                resultList.add(result);
            }
            return resultList;
        } catch (Exception exception) {
            // 当查询或数据封装过程中发生异常时，抛出自定义异常信息
            throw new RuntimeException(
                    "\r\n从 MySQL R7 数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

    /**
     * 从 MySQL R7 数据库中查询数据，并将结果转换为指定类型的列表
     *
     * @param sql            查询的SQL语句
     * @param underScoreToCamel  是否将下划线风格的字段名转换为驼峰风格
     * @param clz            指定的结果对象类型
     * @return               查询结果列表，列表中的每个元素都是指定类型clz的实例
     * @throws RuntimeException 当查询数据库失败时抛出运行时异常
     */
    public static <T> List<T> queryList(String sql, boolean underScoreToCamel, Class<T> clz) {
        try {
            // 执行SQL查询，获取结果列表，每个结果是一个字段-值的映射
            List<Map<String, Object>> mapList = MySQLR7Util.getJdbcTemplate().queryForList(sql);
            List<T> resultList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                Set<String> keys = map.keySet();
                // 当返回的结果中存在数据，通过反射将数据封装成样例类对象
                T result = clz.newInstance();
                for (String key : keys) {
                    // 根据underScoreToCamel参数决定是否将字段名从下划线风格转换为驼峰风格
                    String propertyName = underScoreToCamel ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key) : key;
                    // 使用Apache Commons BeanUtils工具库设置对象属性值
                    BeanUtils.setProperty(
                            result,
                            propertyName,
                            map.get(key)
                    );
                }
                resultList.add(result);
            }
            return resultList;
        } catch (Exception exception) {
            // 当查询数据库失败时，抛出包含详细错误信息的RuntimeException
            throw new RuntimeException(
                    "\r\n从 MySQL R7 数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

}
