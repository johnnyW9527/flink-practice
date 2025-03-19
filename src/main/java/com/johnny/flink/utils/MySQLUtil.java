package com.johnny.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/18 20:19
 */
public class MySQLUtil {

    public static Logger logger = LoggerFactory.getLogger(MySQLUtil.class);

    private JdbcTemplate jdbcTemplate;

    public MySQLUtil(String url, String username, String password, int maxConnect, int minConnect) {
        initJdbcTemplate(url, username, password, maxConnect, minConnect);
    }

    public MySQLUtil(String url, String username, String password) {
        initJdbcTemplate(url, username, password, 2, 1);
    }

    /**
     * 初始化JdbcTemplate方法，使用HikariCP连接池
     *
     * @param url 数据库连接URL
     * @param username 数据库用户名
     * @param password 数据库密码
     * @param maxConnect 连接池最大连接数
     * @param minConnect 连接池最小空闲连接数
     */
    public void initJdbcTemplate(String url, String username, String password, int maxConnect, int minConnect) {
        try {
            // 创建HikariDataSource实例
            HikariDataSource ds = new HikariDataSource();
            // 简短暂停，以避免快速连续访问数据库
            Thread.sleep(1000);
            // 设置JDBC驱动类名
            ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
            // 设置数据库连接URL
            ds.setJdbcUrl(url);
            // 设置数据库用户名
            ds.setUsername(username);
            // 设置数据库密码
            ds.setPassword(password);
            // 设置连接池最大连接数
            ds.setMaximumPoolSize(maxConnect);
            // 设置连接池最小空闲连接数
            ds.setMinimumIdle(minConnect);
            // 使用HikariDataSource创建JdbcTemplate实例
            jdbcTemplate = new JdbcTemplate(ds);

            // 日志记录JdbcTemplate初始化成功信息
            logger.info(
                    "使用HikariPool连接池初始化JdbcTemplate成功，使用的URL为：{} , 其中最大连接大小为：{} , 最小连接大小为：{} ;",
                    ds.getJdbcUrl(),
                    ds.getMaximumPoolSize(),
                    ds.getMinimumIdle()
            );
        } catch (Exception e) {
            // 抛出运行时异常，包含MySQL数据库的jdbcTemplate创建失败的错误信息
            throw new RuntimeException("创建MySQL数据库的jdbcTemplate失败，抛出的异常信息为：" + e.getMessage());
        }
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * 处理特殊字符
     * 该方法主要用于处理输入数据中的特殊字符，以确保数据可以在特定的上下文中正确处理
     * 例如，处理SQL查询中的单引号问题，避免SQL注入等安全问题
     *
     * @param object 传入的对象，可以是字符串或其他类型的对象
     * @return 处理后的字符串，其中特殊字符被转义或移除
     */
    public String disposeSpecialCharacter(Object object) {

        // 根据传入的情况，将数据转换成json格式（如果传入为string，那就本来是json格式，不需要转）
        String data;
        if (object instanceof String) {
            data = object.toString();
        } else {
            data = JSON.parseObject(JSON.toJSONString(object)).toString();
        }

        // 处理传入数据中的特殊字符（例如： 单引号）
        data = data.replace("'", "''");

        // 将其中为空值的去掉（注意：如果不能转换成json并从中获取数据的，就是从delete中传过来的，只有单纯的value值）
        try {
            JSONObject result = new JSONObject();
            for (Map.Entry<String, Object> entry : JSON.parseObject(data).entrySet()) {
                if (StringUtils.isNotEmpty(entry.getValue().toString())) {
                    result.put(entry.getKey(), entry.getValue());
                }
            }
            data = result.toJSONString();
        } catch (Exception exception) {
            logger.warn("传入的数据为：{}；该数据是从delete中传入的，不能转换成json值", object);
        }
        return data;

    }

    /**
     * 查询数据库中的数据列表，并将结果转换为指定类型的对象列表
     *
     * @param sql SQL查询语句
     * @param clz 指定的结果对象类型
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @return 一个包含指定类型对象的列表
     *
     * 此方法通过JDBC模板执行SQL查询，并将查询结果转换为一个包含指定类型对象的列表
     * 它支持将数据库中的字段名从下划线风格转换为Java中的驼峰风格，以便与Java对象的属性名匹配
     */
    public <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        try {
            // 执行SQL查询，获取结果列表，每个结果为一个字段-值的映射
            List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
            List<T> resultList = new ArrayList<>();
            for (Map<String, Object> map : mapList) {
                Set<String> keys = map.keySet();
                // 当返回的结果中存在数据，通过反射将数据封装成样例类对象
                T result = clz.newInstance();
                for (String key : keys) {
                    // 根据配置，决定是否将字段名从下划线风格转换为驼峰风格
                    String propertyName = underScoreToCamel ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key) : key;
                    // 使用Apache Commons BeanUtils工具类设置对象属性值
                    BeanUtils.setProperty(result, propertyName, map.get(key));
                }
                resultList.add(result);
            }
            return resultList;
        } catch (Exception exception) {
            // 抛出运行时异常，包含详细的错误信息和查询的SQL语句
            throw new RuntimeException(
                    "\r\n从MySQL数据库中 查询 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n查询的SQL为：" + sql
            );
        }
    }

    /**
     * 向指定的数据库表中插入数据
     *
     * @param tableName 数据库表名，用于指定数据插入的目标表
     * @param underScoreToCamel 指示是否将字段名从下划线风格转换为驼峰风格
     * @param object 包含要插入数据的对象，其属性对应数据库表的字段
     */
    public void insert(String tableName, boolean underScoreToCamel, Object object) {

        // 将传入的对象转换成JSONObject格式（并将其中的特殊字符进行替换）
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

        // 从传入的数据中获取出对应的key和value，因为要一一对应，所以使用list
        ArrayList<String> fieldList = Lists.newArrayList(data.keySet());
        ArrayList<String> valueList = new ArrayList<>();
        for (String field : fieldList) {
            valueList.add(data.getString(field));
        }

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO ").append(tableName);
        sql.append(" ( ");
        for (String field : fieldList) {
            // 根据underScoreToCamel参数决定字段名是否转换为下划线风格
            if (underScoreToCamel) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field)).append(",");
            } else {
                sql.append(field).append(",");
            }
        }
        // 移除最后一个逗号
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" ) ");
        // 使用拼接好的值列表构建SQL的values部分
        sql.append(" values ('").append(StringUtils.join(valueList, "','")).append("')");

        // 执行插入操作
        try {
            jdbcTemplate.execute(sql.toString());
        } catch (Exception exception) {
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 插入 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 从MySQL数据库中删除数据
     *
     * @param tableName 数据表名
     * @param fieldNameAndValue 字段名和对应的值，用于拼接删除条件
     * @return 删除操作影响的行数
     * @throws RuntimeException 如果删除条件为空，抛出异常防止全表删除
     */
    public int delete(String tableName, Map<String, Object> fieldNameAndValue) {

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" delete from ").append(tableName);
        if (!fieldNameAndValue.isEmpty()) {
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                sql.append(fieldNameAndValueEntry.getKey()).append(" = ").append("'")
                        .append(disposeSpecialCharacter(fieldNameAndValueEntry.getValue()))
                        .append("'")
                        .append(" AND ");
            }
            sql.delete(sql.length() - 4, sql.length() - 1);
        } else {
            throw new RuntimeException("从MySQL中删除数据异常，输入的删除条件没有指定字段名和对应的值，会进行全表删除， 拼接的SQL为：" + sql);
        }

        // 执行删除操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 删除 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 根据指定条件删除数据库中的数据
     *
     * @param tableName 数据表名
     * @param underScoreToCamel 是否将下划线命名转换为驼峰命名
     * @param object 包含删除条件的对象
     * @param fields 需要用于删除条件的字段
     * @return 删除操作的结果，通常为受影响的行数
     */
    public int delete(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            if (underScoreToCamel) {
                // data中的均为驼峰，获取数据时需要使用驼峰；但是将数据写入到fieldNameAndValue中时，需要全部转换成下划线
                fieldNameAndValue.put(field.contains("_") ? field : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, field),
                        data.getString(field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field)
                );
            } else {
                // data中均为下划线，field中也是下划线
                fieldNameAndValue.put(field, data.getString(field));
            }
        }
        return delete(tableName, fieldNameAndValue);

    }

    /**
     * 更新数据库表中的数据
     *
     * @param tableName 数据库表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要更新的数据对象
     * @param fieldNameAndValue 更新条件的字段名和值的映射
     * @return 更新操作影响的行数
     * @throws RuntimeException 如果更新条件为空或更新操作失败，则抛出运行时异常
     */
    public int update(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 将传入的对象转换成JSONObject格式，并判断输入的数据是否符合更新条件
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));
        if (fieldNameAndValue == null || fieldNameAndValue.isEmpty()) {
            throw new RuntimeException("向MySQL中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + data);
        }
        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" UPDATE ").append(tableName);
        sql.append(" SET ");
        if (underScoreToCamel) {
            // 删除传入对象中要更新的数据
            for (String key : fieldNameAndValue.keySet()) {
                data.remove(key.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key) : key);
            }
            // 拼接要更新的结果值
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey()))
                        .append(" = ")
                        .append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

            // 拼接判断条件
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                String key = fieldNameAndValueEntry.getKey();
                Object value = fieldNameAndValueEntry.getValue();
                sql.append(key.contains("_") ? key : CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key))
                        .append(" = ")
                        .append("'")
                        .append(value)
                        .append("'")
                        .append(" AND ");
            }

        } else {
            // 删除传入对象中要更新的数据
            for (String key : fieldNameAndValue.keySet()) {
                data.remove(key);
            }
            // 拼接要更新的结果值
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                sql.append(entry.getKey())
                        .append(" = ")
                        .append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

            // 拼接判断条件
            sql.append(" WHERE ");
            for (Map.Entry<String, Object> fieldNameAndValueEntry : fieldNameAndValue.entrySet()) {
                String key = fieldNameAndValueEntry.getKey();
                Object value = fieldNameAndValueEntry.getValue();
                sql.append(key)
                        .append(" = ")
                        .append("'")
                        .append(value)
                        .append("'")
                        .append(" AND ");
            }
        }
        sql.delete(sql.length() - 4, sql.length() - 1);

        // 执行更新操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 更新 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }
    }

    /**
     * 更新指定表中的记录
     *
     * @param tableName 数据库表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要更新的记录对象
     * @param fields 指定的字段名数组，用于确定要更新哪些字段
     * @return 更新操作影响的行数
     */
    public int update(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));

        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            // 如果需要将下划线风格转换为驼峰风格，则进行转换
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }
        return update(tableName, underScoreToCamel, object, fieldNameAndValue);

    }

    /**
     * 根据主键执行插入或更新操作（Upsert）
     * 如果存在指定主键的记录，则更新该记录；否则插入新记录
     *
     * @param tableName 数据库表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要插入或更新的数据对象
     * @param fieldNameAndValue 包含主键和对应值的映射，用于确定要更新的记录
     * @return 影响的行数
     * @throws RuntimeException 如果输入的更新条件为空或执行SQL时发生异常
     */
    public int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 判断输入的数据是否符合更新条件
        if (fieldNameAndValue == null || fieldNameAndValue.isEmpty()) {
            throw new RuntimeException("向MySQL中更新数据异常，输入的更新条件没有指定数据，不能更新（这样更新会全表更新），传入的数据为：" + object);
        }

        // 将传入的object转换成json类型，并将传入的更新匹配字段和值（即fieldNameAndValue），添加到数据对象中（即data）
        JSONObject data = JSON.parseObject(JSON.toJSONString(object));
        data.putAll(fieldNameAndValue);
        data = JSON.parseObject(disposeSpecialCharacter(data));

        // 拼接SQL
        StringBuilder sql = new StringBuilder();
        sql.append(" INSERT INTO ").append(tableName);

        // 根据underScoreToCamel参数决定字段名的风格
        if (underScoreToCamel) {
            // 构建插入语句的字段部分（下划线风格）
            sql.append(" ( ");
            for (String key : data.keySet()) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, key)).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            // 构建插入语句的值部分（下划线风格）
            sql.append(" values ");

            sql.append(" ( ");
            for (Object value : data.values()) {
                sql.append("'").append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, value.toString())).append("'").append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            // 构建更新语句部分（下划线风格）
            sql.append(" ON DUPLICATE KEY UPDATE ");

            for (Map.Entry<String, Object> entry : fieldNameAndValue.entrySet()) {
                sql.append(CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, entry.getKey())).append(" = ").append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
        } else {
            // 构建插入语句的字段部分（原风格）
            sql.append(" ( ");
            for (String key : data.keySet()) {
                sql.append(key).append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");

            // 构建插入语句的值部分（原风格）
            sql.append(" values ");

            sql.append(" ( ");
            for (Object value : data.values()) {
                sql.append("'").append(value.toString()).append("'").append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" ) ");
            // 构建更新语句部分（原风格）
            sql.append(" ON DUPLICATE KEY UPDATE ");
            for (Map.Entry<String, Object> entry : fieldNameAndValue.entrySet()) {
                sql.append(entry.getKey())
                        .append(" = ")
                        .append("'")
                        .append(entry.getValue())
                        .append("'")
                        .append(",");
            }
            sql.deleteCharAt(sql.length() - 1);
        }

        // 执行upsert操作
        try {
            return jdbcTemplate.update(sql.toString());
        } catch (Exception exception) {
            throw new RuntimeException(
                    "\r\n向MySQL数据库中 upsert 数据失败，" +
                            "\r\n抛出的异常信息为：" + exception.getMessage() +
                            "\r\n执行的SQL为：" + sql
            );
        }

    }

    /**
     * 根据主键更新或插入数据
     * 如果记录不存在，则插入新记录；如果记录存在，则更新记录
     *
     * @param tableName 数据库表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要更新或插入的数据对象
     * @param fields 主键字段数组
     * @return 影响的行数
     */
    public int upsertByPrimaryKey(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 将传入的对象转换成JSONObject格式
        JSONObject data = JSON.parseObject(disposeSpecialCharacter(object));
        // 根据传入的字段，获取要更新的主键值
        HashMap<String, Object> fieldNameAndValue = new HashMap<>();
        for (String field : fields) {
            // 如果需要将下划线风格的字段名转换为驼峰风格，则进行转换
            if (underScoreToCamel) {
                field = field.contains("_") ? CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, field) : field;
            }
            fieldNameAndValue.put(field, data.getString(field));
        }
        return upsertByPrimaryKey(tableName, underScoreToCamel, object, fieldNameAndValue);

    }


    /**
     * 更新或插入数据库中的数据
     * 如果更新操作影响的行数为0，则执行插入操作
     * 此方法用于处理数据库中的“ Upsert”操作，即“更新或插入”
     * 当更新操作未影响任何行时，假定记录不存在并尝试插入
     *
     * @param tableName 数据库表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要更新或插入的数据对象
     * @param fields 指定的字段名数组，用于更新或插入操作
     * @return 更新或插入操作影响的行数
     */
    public int upsert(String tableName, boolean underScoreToCamel, Object object, String... fields) {

        // 尝试更新数据库中的数据
        int updateNum = update(tableName, underScoreToCamel, object, fields);

        // 如果更新操作未影响任何行，执行插入操作
        if (updateNum == 0) {
            insert(tableName, underScoreToCamel, object);
            updateNum = 1;
        }
        // 返回影响的行数
        return updateNum;
    }

    /**
     * 根据条件执行更新或插入操作
     * 此方法首先尝试更新表中的记录如果更新失败（即没有找到要更新的记录），则执行插入操作
     *
     * @param tableName 表名
     * @param underScoreToCamel 是否将下划线风格的字段名转换为驼峰风格
     * @param object 要更新或插入的对象
     * @param fieldNameAndValue 字段名和对应的值的映射，用于确定要更新的记录
     * @return 更新或插入的记录数如果更新失败但插入成功，则返回1
     */
    public int upsert(String tableName, boolean underScoreToCamel, Object object, Map<String, Object> fieldNameAndValue) {

        // 尝试更新表中的记录
        int updateNum = update(tableName, underScoreToCamel, object, fieldNameAndValue);
        // 如果更新失败（即没有找到要更新的记录），则执行插入操作
        if (updateNum == 0) {
            insert(tableName, underScoreToCamel, object);
            updateNum = 1;
        }
        return updateNum;
    }


}
