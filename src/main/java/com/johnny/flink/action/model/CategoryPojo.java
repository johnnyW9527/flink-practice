package com.johnny.flink.action.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/14 17:25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CategoryPojo {
    //分类名称
    private String category;
    //该分类总销售额
    private double totalPrice;
    // 截止到当前时间的时间,本来应该是EventTime,但是我们这里简化了直接用当前系统时间即可
    private String dateTime;

}
