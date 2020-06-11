package com.githubzhu.ecommerceuserprofile.etls.platformetl;

import com.sun.deploy.panel.ITreeNode;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.w3c.dom.css.Counter;
import scala.Tuple2;

import java.util.List;

/**
 * @Author: github_zhu
 * @Describtion:
 * @Date:Created in 2020/6/10 19:29
 * @ModifiedBy:
 */
public class HotWordEtl {
    public static void main(String[] args) {
    //创建一个java spark context ，方便以后调用treansform
        SparkConf sparkConf = new SparkConf()
                .setAppName("hot word elt")
                .setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //首先从hdfs 读取数据
        System.setProperty("HADOOP_USER_NAME", "atguiug");
        //JavaRDD<String> lineRDD = jsc.textFile("hdfs://hadoop102:9000/data/SougouQ.sample.txt");
        JavaRDD<String> lineRDD = jsc.textFile("hdfs://192.168.5.102:9000/data/SogouQ.sample.txt");

        //2.mapTopair 得到二元组，准备 wordcount
        JavaPairRDD<String, Integer> pairRDD = lineRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String word = s.split("\t")[2];
                return new Tuple2<>(word, 1);
            }
        });

        //3. 以word 作为key 进行reduce聚合操作
        JavaPairRDD<String, Integer> countRDD= pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //4. 元素互换位置
        JavaPairRDD<Integer, String> swapRDD = countRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        //5.按照count 排序
        JavaPairRDD<Integer, String> sortedRDD = swapRDD.sortByKey(false);

        //6.再互换位置到之前的状态，提取TopN,得到一个List

        List<Tuple2<String, Integer>> resuultList = sortedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        }).take(10);

        //打印输出
        for (Tuple2<String, Integer> hotWordCount : resuultList) {
            System.out.println(hotWordCount._1+"===count:"+hotWordCount._2);
        }
    }


}
