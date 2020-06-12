package com.githubzhu.ecommerceuserprofile.etls.platformetl;

import com.githubzhu.ecommerceuserprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/11 11:12
 * @ModifiedBy:
 */
public class ConversionEtl {
    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();
        ConversionVo coversionVo = behaviorConversionCountEtl(session);

        System.out.println(coversionVo);

    }

    private static ConversionVo behaviorConversionCountEtl(SparkSession session) {
        //1.查询order表，提取下单用户
        Dataset<Row> orderMember = session.sql(
                "select distinct(member_id)from ecommerce.t_order where order_status = 2");
        //2.查询order 表 ，提取复购的用户
        Dataset<Row> orderAgainMember = session.sql("select t.member_id as member_id" +
                " from (select count(order_id) as orderCount, member_id from ecommerce.t_order " +
                " where order_status = 2 group by member_id) as t where t.orderCount >= 2");
        //3. 查询coupon_member表 提取购买优惠券的用户
        Dataset<Row> chargeMember = session.sql(
                "select distinct(member_id) as member_id from ecommerce.t_coupon_member " +
                        "where coupon_id !=1 and coupon_channel = 1");

        //因为 储值的用户，不一定是复购的用户，所以 做转化率分析时候取交集
        Dataset<Row> chargeOrderAgainMember = chargeMember.join(
                orderAgainMember,
                orderAgainMember.col("member_id").equalTo(chargeMember.col("member_id")),
                "inner"
        );

        //统计各层的数量
        long orderCount = orderMember.count();
        long orderAgainCount = orderAgainMember.count();
        long chargeCount = chargeOrderAgainMember.count();

        //包装成VO
        ConversionVo conversionVo = new ConversionVo();
        conversionVo.setView(1000L);
        conversionVo.setClick(800L);
        conversionVo.setOrder(orderCount);
        conversionVo.setOrderAgain(orderAgainCount);
        conversionVo.setChargeCoupon(chargeCount);

        return conversionVo;
    }

    //定义vo 保存当前平台用户某种行为的总量
    @Data
    static class ConversionVo {
        private Long view; //浏览行为
        private Long click; //点击行为（查看详情）
        private Long order; //下单购买
        private Long orderAgain; //复购行为
        private Long chargeCoupon; //购买优惠券

    }


}
