package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class JavaWordCount {
    public static void main(String[] args) {
        // 1. 配置Spark环境
        SparkConf conf = new SparkConf()
                .setAppName("JavaWordCount")
                .setMaster("local[*]"); // 本地运行模式

        // 2. 创建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            // 3. 读取输入文件（本地文件或HDFS文件路径）
            String inputPath = args.length > 0 ? args[0] : "input.txt";
            JavaRDD<String> inputRDD = sc.textFile(inputPath);

            // 4. 词频统计核心逻辑（关键修复：flatMap返回List<String>）
            JavaPairRDD<String, Integer> wordCounts = inputRDD
                    // 修复点：直接返回List<String>（Iterable的实现类），不调用iterator()
                    .flatMap(line -> Arrays.asList(line.split("\\s+"))) // 分割后转List，直接返回
                    .mapToPair(word -> new Tuple2<>(word.trim(), 1)) // 去除单词前后空格，避免空字符
                    .reduceByKey((count1, count2) -> count1 + count2); // 按单词分组累加

            // 5. 输出结果
            String outputPath = args.length > 1 ? args[1] : "output";
            wordCounts.saveAsTextFile(outputPath); // 保存到文件（HDFS或本地）

            // 可选：本地打印结果（测试用）
            List<Tuple2<String, Integer>> resultList = wordCounts.collect();
            for (Tuple2<String, Integer> tuple : resultList) {
                System.out.println(tuple._1() + ": " + tuple._2());
            }

        } finally {
            // 6. 关闭Spark上下文（必须执行，释放资源）
            sc.close();
        }
    }
}
