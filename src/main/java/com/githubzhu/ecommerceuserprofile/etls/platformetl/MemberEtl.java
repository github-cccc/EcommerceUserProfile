package com.githubzhu.ecommerceuserprofile.etls.platformetl;

import com.alibaba.fastjson.JSON;
import com.githubzhu.ecommerceuserprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/9 19:47
 * @ModifiedBy:
 */
public class MemberEtl {

    public static void main(String[] args) {
        //初始化 spark session
        SparkSession session = SparkUtils.initSession();

        //2. 写SQL 查询想要的数据
        List<MemberSex> memberSexs = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeats = memberHeatEtl(session);

        //3.拼成想要展示的成果，提供给前端页面
        MemberVo memberVo = new MemberVo();
        memberVo.setMemberSexs(memberSexs);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberHeat(memberHeats);

        //直接控制台打印输出
        System.out.println(JSON.toJSONString(memberVo));

    }

    //统计平台用户性别分布
    public static List<MemberSex> memberSexEtl(SparkSession session) {

        //写SQL，查询得到一个DataSet
        Dataset<Row> dataset = session.sql("select sex as memberSex, count(id) as sexCount " +
                " from ecommerce.t_member group by sex");

        //将dataset 转换成List
        List<String> list = dataset.toJSON().collectAsList();

        //将List 转换成流 ，进行每一行数据的遍历 ，转换成 MemberSex
        List<MemberSex> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());
        return result;
    }

    //用户渠道的分布统计
    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        //写SQL，查询得到一个DataSet
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel, count(id) as channelCount " +
                " from ecommerce.t_member group by member_channel");

        //将dataset 转换成List
        List<String> list = dataset.toJSON().collectAsList();

        //将List 转换成流 ，进行每一行数据的遍历 ，转换成 MemberSex
        List<MemberChannel> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());
        return result;
    }

    //媒体平台关注分布统计
    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {

        //写SQL，查询得到一个DataSet
       /* 问题SQL
        Dataset<Row> dataset = session.sql("select count(if(mp_open_id != 'null',true,null)) as subCount," +
                " count(if(mp_open_id = 'null',true,null)) as unSubCount" +
                " from ecommerce.t_member");*/

        Dataset<Row> dataset = session.sql("select sex as memberSex, count(id) as sexCount " +
                " from ecommerce.t_member group by sex");

        //将dataset 转换成List
        List<String> list = dataset.toJSON().collectAsList();

        //将List 转换成流 ，进行每一行数据的遍历 ，转换成 MemberSex
        List<MemberMpSub> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());
        return result;

    }

    public static MemberHeat memberHeatEtl(SparkSession session) {
        //reg complete 从用户表中提取
        /*问题SQL
        Dataset<Row> reg_complete_count = session.sql(
                "select count(if(phone ='null',true,null)) as reg ," +
                        "count(if (phone != 'null',true, null)) as complete " +
                        "from ecommerce.t_member");*/

        Dataset<Row> reg_complete_count = session.sql(
                "select count(if(phone = 'null', true, null)) as reg, " +
                        " count(if(phone != 'null', true, null)) as complete " +
                        " from ecommerce.t_member");

        //order ,orderAgain 从订单表中 提取
        Dataset<Row> order_andAagain_count = session.sql(
                "select count(if(t.orderCount = 1, true, null)) as order, " +
                        " count(if(t.orderCount >= 2, true, null)) as orderAgain " +
                        " from (select count(order_id) as orderCount, member_id from ecommerce.t_order group by member_id) as t");
        /*问题sql
                "select count (if(t.orderCount = 1,true,null)) as order ," +
                        "count(if(t.orderCount>=2,true,null)) as orderAgain" +
                        "from (select count(order_id) as orderCount,member_id from ecommerce.t_order group by member_id)as t")*/
        //coupon ,从 cupon_member 表中提取  对member_id 进行去重，然后count
        Dataset<Row> coupon_count = session.sql("select count(distinct member_id) as coupon from ecommerce.t_coupon_member"
                );
        //"select count(distinct member_id) as coupon from emmoerce.t_coupon_member"   问题Sql


        //最终将三个查询结果连接在一起，做cross join
        Dataset<Row> heat = coupon_count.crossJoin(reg_complete_count).crossJoin(order_andAagain_count);

        List<String> list = heat.toJSON().collectAsList();

        //将list转换成流，进行每一行数据的遍历 转换炒年糕Memberheat
        List<MemberHeat> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());
        return result.get(0);
    }

    //定义一个最终想要生成的VO，用来展示饼图的数据信息
    @Data
    static class MemberVo {
        //有四部分组成
        private List<MemberSex> memberSexs;  //性别信息统计
        private List<MemberChannel> memberChannels; //渠道信息统计
        private List<MemberMpSub> memberMpSubs; //是否关注媒体平台统计
        private MemberHeat memberHeat; //用户热度统计
    }

    @Data
    static class MemberSex {
        private Integer memberSex;//性别编号
        private Integer sexCount;//当前性别的count 数量
    }

    @Data
    static class MemberChannel {
        private Integer memberChannel; //渠道编号
        private Integer channelCount; //渠道数量统计
    }

    @Data
    static class MemberMpSub {

        private Integer subCount; //关注平台统计
        private Integer ubSubCoubt; //未关注
    }

    @Data
    static class MemberHeat {

        private Integer reg; //只注册但未填写手机号的用户统计数
        private Integer complete; //完善信息，填写手机号的用户统计
        private Integer order; //下过单的用户数统计
        private Integer orderAgain; // 复购 用户计数
        private Integer coupon; //购买过 消费券的用户统计数

    }

}
