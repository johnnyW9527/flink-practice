package com.johnny.flink.webproject.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/4/1 09:50
 */
@Data
@AllArgsConstructor
public class LoginEvent {

    private Long userId;

    private String ip;

    private String loginState;

    private Long timestamp;


}
