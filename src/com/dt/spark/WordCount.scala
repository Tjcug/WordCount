package com.dt.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by dell-pc on 2016/3/6.
  */
object WordCount {
  def main(args: Array[String]) {
    /*  第一步：创建spark配置对象 SparkConf 设置sprak程序运行时的配置信息
     *  列入说通过setMaster来自设置程序要连接的spark集群的Master的URL，如果设置为local，
     *  则表示sprak程序在本地运行，特别适合集群配置条件差的学长
     */

    val conf= new SparkConf()  //创建SparkConf对象
    conf.setAppName("my First Spark APP！！") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local")//此时程序在本地运行，不需要安装Spark集群

    /*
     *第二步：创建SparkContext对象
     * SparkContext是Spark程序所有功能的唯一入口，无论是采用scala，java，python等都必须有一个SparkContext实列
     * SparkContext核心作用：Spark应用程序所需要的核心组件，包括DAGSchedule
     * 同时还会辅助SPark程序往Master注册程序等
     * SparkContext是整个Spark应用程序中最为至关重要的一个对象
     */

    val sc= new SparkContext(conf) //创建SparkContext对象，通过传入SparkContext实列来定制Spark运行的具体参数和配置信息

    /*
     * 第三步： 根据具体的数据来源（HDFS\HBase\Local FS\DB\ S3 等) 通过SParkCOntext来创建RDD
     * RDD的创建基本有三个方式：根据外部的数据来源（列入HDFS）、根据scala集合、有其他的RDD操作
     * 数据会被RDD分成一系列的Partitions，分配到每一个Partition的数据属于一个Task的处理范畴
     */

    val lines =sc.textFile("E://TDDOWNLOAD//BigDataSpark//spark-1.6.0-bin-hadoop2.6//README.md",1) //读取文件并设置为一个

    /*
     * 第四步： 对初始的RDD进行Transformation级别的处理，列入 map、filter 等高阶函数的编程，俩进行具体的数据计算
     * 第4.1步将没一个的字符串拆分成若干个单词
     */

    val words= lines.flatMap{ line => line.split(" ")} //对每一行的字符串进行单词拆分并把所有行的拆分结果通过flat合并成一个大的集合

    /*
     * 第4.2步 在拆分单词的基础上对每一个单词实列计数为1 ，页就是word=>（word,1）
     */

    val pairs=words.map{ word => (word,1)}

    /*
     * 第4.3步 在每一个单词实列计数为1的基础上统计每一个单词在文件中出现的总次数
     */

    val wordCount = pairs.reduceByKey(_+_) //对相同的key，进行value的累加（包括local和Reduce级别的同时Reduce）

    wordCount.collect.foreach(wordNumberPair => println(wordNumberPair._1 +" : " + wordNumberPair._2)) //打印出最后结果

    sc.stop()
  }
}
