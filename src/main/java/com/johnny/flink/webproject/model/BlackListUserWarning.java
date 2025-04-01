package com.johnny.flink.webproject.model;

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
 * @date 2025/3/25 20:09
 */
@AllArgsConstructor
@Data
@NoArgsConstructor
public class BlackListUserWarning {

    private Long userId;
    private Long adId;
    private String warningMsg;

}
