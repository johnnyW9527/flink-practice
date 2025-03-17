package com.johnny.flink.advance.model;

import lombok.Data;

/**
 * <b>订单明细</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 20:25
 */
@Data
public class OrderItem {

    private String goodsId;
    private String itemId;
    private Integer count;

    @Override
    public String toString() {
        return "OrderItem{" +
                "goodsId='" + goodsId + '\'' +
                ", itemId='" + itemId + '\'' +
                ", count=" + count +
                '}';
    }
}
