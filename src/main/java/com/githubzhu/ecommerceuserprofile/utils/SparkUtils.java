package com.githubzhu.ecommerceuserprofile.utils;

import org.apache.spark.sql.SparkSession;

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/9 19:27
 * @ModifiedBy:
 */
public class SparkUtils {
    //定义一个spark session 的会话
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    //初始化 spark session 的方法
    public static SparkSession initSession() {
        //先判断会话池 中是否有session ，如果有直接用，没有再创建
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }
        SparkSession session = SparkSession.builder()
                .appName("userprofile-etl")
                .master("local[*]")
                .config("es.nodes", "hadoop102")
                .config("es.port", "9200")
                .config("es.index.auto.create", "false")
                .enableHiveSupport()    // 启用hive支持
                .getOrCreate();
        sessionPool.set(session);
        return session;
    }

}
