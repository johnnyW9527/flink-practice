package com.johnny.flink.advance.model;

import com.johnny.flink.advance.join.JoinDemo01;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * <b>请输入名称</b>
 * <p>
 * 描述<br/>
 * 作用：；<br/>
 * 限制：；<br/>
 * </p>
 *
 * @author wan.liang(79274)
 * @date 2025/3/17 20:26
 */
@Data
public class Goods {

    private String goodsId;
    private String goodsName;
    private BigDecimal goodsPrice;

    public static List<Goods> GOODS_LIST;
    public static Random random;

    static {

        random = new Random();
        GOODS_LIST = new ArrayList<>();
        GOODS_LIST.add(new Goods("1", "小米12", new BigDecimal(4890)));
        GOODS_LIST.add(new Goods("2", "iphone12", new BigDecimal(12000)));
        GOODS_LIST.add(new Goods("3", "MacBookPro", new BigDecimal(15000)));
        GOODS_LIST.add(new Goods("4", "Thinkpad X1", new BigDecimal(9800)));
        GOODS_LIST.add(new Goods("5", "MeiZu One", new BigDecimal(3200)));
        GOODS_LIST.add(new Goods("6", "Mate 40", new BigDecimal(6500)));
    }

    public Goods() {}

    public static Goods randomGoods() {
        return GOODS_LIST.get(random.nextInt(GOODS_LIST.size()));
    }

    public Goods(String goodsId, String goodsName, BigDecimal goodsPrice) {
        this.goodsId = goodsId;
        this.goodsName = goodsName;
        this.goodsPrice = goodsPrice;
    }

    @Override
    public String toString() {
        return "Goods{" +
                "goodsId='" + goodsId + '\'' +
                ", goodsName='" + goodsName + '\'' +
                ", goodsPrice=" + goodsPrice +
                '}';
    }
}
