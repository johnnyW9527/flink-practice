package com.johnny.flink.advance.model;

import lombok.Data;

import java.math.BigDecimal;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 20:29
 */
@Data
public class FactOrderItem {

    private String goodsId;
    private String goodsName;
    private BigDecimal count;
    private BigDecimal totalMoney;

    @Override
    public String toString() {
        return "FactOrderItem{" +
                "goodsId='" + goodsId + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", count=" + count +
                ", totalMoney=" + totalMoney +
                '}';
    }
}
