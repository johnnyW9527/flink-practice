package com.johnny.flink.model;

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
 * @date 2025/2/25 10:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {

    private Integer id;

    private String name;

    private Integer age;

}
