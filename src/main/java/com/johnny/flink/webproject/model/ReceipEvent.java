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
 * @date 2025/4/1 10:38
 */
@Data
@AllArgsConstructor
public class ReceipEvent {

    private String txId;
    private String payChannel;
    private Long timestamp;

}
