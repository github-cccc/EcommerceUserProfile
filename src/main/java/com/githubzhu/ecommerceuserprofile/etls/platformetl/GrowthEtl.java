package com.githubzhu.ecommerceuserprofile.etls.platformetl;

import com.alibaba.fastjson.JSONObject;
import com.githubzhu.ecommerceuserprofile.utils.SparkUtils;
import com.githubzhu.ecommerceuserprofile.utils.dateutils.DateStyle;
import com.githubzhu.ecommerceuserprofile.utils.dateutils.DateUtil;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;


import java.math.BigDecimal;
import java.nio.channels.SeekableByteChannel;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/10 20:31
 * @ModifiedBy:
 */
public class GrowthEtl {
    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();
        //自定义方法 ，提取每一天的数据，作为list 返回

        List<EveryDayCountVo> everyDayCountVoList = growthEtl(session);

        //打印输出
        System.out.println(everyDayCountVoList);
    }

    @Data
    static class EveryDayCountVo {
        private String day;//当前的日期
        private Integer newRegCount; //当天新增注册人数
        private Integer totalMemberCount; //当天为止，平台总人数
        private Integer totalOrderCount; //当天为止，平台的总订单数
        private BigDecimal gmv; //当天为止， 平台订单交易的总流水金额
    }

    private static List<EveryDayCountVo> growthEtl(SparkSession session) {
        //因为是近7天之内，所以要有日期的判断
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date sevenDayBefor = DateUtil.addDay(nowDay, -7);

        //统计近7天 每天用户注册（新增用户数和总量）
        String memberEtlSql =  "select date_format(create_time, 'yyyy-MM-dd') as day, " +
                " count(id) as newRegCount, max(id) as totalMemberCount " +
                " from ecommerce.t_member where create_time >= '%s' " +
                " group by date_format(create_time, 'yyyy-MM-dd') order by day";

                /*"select date_format(create_time,'yyyy-MM-dd')as day," +
                "count(id) as newRegCount , max(id) as totalMemberCount, " +
                "from ecommerce.t_member where create_time >= '%s'" +
                "group by date_formate(create_time,'yyyy-MM-dd') order by day";*/
        memberEtlSql = String.format(memberEtlSql, DateUtil.DateToString(sevenDayBefor, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> memberRegDs = session.sql(memberEtlSql);

        //2.统计近7天每天订单数量和总流水

        String orderEtlSql = "select date_format(create_time,'yyyy-MM-dd') as day," +
                "max(order_id) as totalOrderCount, sum(origin_price) as gmv " +
                "from ecommerce.t_order where create_time >= '%s'" +
                "group by date_format(create_time,'yyyy-MM-dd') order by day" ;

              /*  问题SQL
                "select date_format(create_time,'yyyy-MM-dd')as day," +
                "max(order_id) as totalOrderCount,sum(origin_price) as gmv" +
                "from ecommerce.t_order where create_time >= '%s'" +
                "group by date_formate(create_time,'yyyy-MM-dd') order by day ";*/

        orderEtlSql = String.format(orderEtlSql, DateUtil.DateToString(sevenDayBefor, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> memberOrderDs = session.sql(orderEtlSql);

        //连接两个查询结果 ，合并成一个Dataset ，转换包装成Vo
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberRegDs.joinWith(
                memberOrderDs,
                memberRegDs.col("day").equalTo(memberOrderDs.col("day"))
                , "inner"
        );
//先转换成list 然后遍历  取出每一天的数据 ，转成VO ，放入新的list
        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();
        List<EveryDayCountVo> everyDayCountVos = new ArrayList<>();

        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            //先拿每一天数据，
            Row row1 = tuple2._1;
            Row row2 = tuple2._2;

            //定义一个JSONObject，用来存储VO里的每个字段
            JSONObject object = new JSONObject();

            //提取Row 类型里的所有字段
            String[] fields = row1.schema().fieldNames();
            for (String field : fields) {
                Object value = row1.getAs(field);
                object.put(field, value);
            }

            fields = row2.schema().fieldNames();
            for (String field : fields) {
                Object value = row2.getAs(field);
                object.put(field, value);
            }

            EveryDayCountVo everyDayCountVo = object.toJavaObject(EveryDayCountVo.class);
            everyDayCountVos.add(everyDayCountVo);
        }

        //4.求出7天前，再之前的所有订单综合
        String preGmvSql = "select sum(origin_price)as totalGmv from ecommerce.t_order where create_time < '%s'";
        preGmvSql = String.format(preGmvSql, DateUtil.DateToString(sevenDayBefor, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> preGmvDs = session.sql(preGmvSql);
        //只有一个Double 类型的结果，取出来

        double previousGmv = preGmvDs.collectAsList().get(0).getDouble(0);
        BigDecimal preGmv = BigDecimal.valueOf(previousGmv);

        //遍历之前得到每天数据，在preGmv d的基础上叠加，得到gmv  的总量
        BigDecimal currentGmv = preGmv;
        for (int i = 0; i < everyDayCountVos.size(); i++) {
            //货取每天数据
            EveryDayCountVo everyDayCountVo = everyDayCountVos.get(i);
            currentGmv = currentGmv.add(everyDayCountVo.getGmv()); //加上当前day的闺女新增量

            everyDayCountVo.setGmv(currentGmv);
        }

        return everyDayCountVos;
    }
}
