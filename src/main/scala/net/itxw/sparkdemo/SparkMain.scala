package com.ajia.sparkdemo

import java.util.Properties

import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * spark demo 计算所有学校总得分的排名
  */
//class定义类和java一样要new才能调用，object定义类相当于static 单例 可以直接调用，case class自动构造方法和get set方法
object SparkMain {

  /**
    *
    * @param args 参数0-{取多少条数据}
    */
  def main(args: Array[String]): Unit = {

    try {
      //获取参数 开发模式
      val isDev = if (args.length >= 1) args(0) == "dev" else false

      //初始化spark
      val spark = if (isDev) {
        System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.3")

        SparkSession.builder().appName("Sparkdemo")
          //.config("spark.some.config.option", "some-value")
          .config("spark.debug.maxToStringFields", "100")
          .master("local[*]")
          .enableHiveSupport()
          .getOrCreate()
      } else {
        SparkSession.builder().appName("spark-demo")
          //.config("spark.some.config.option", "some-value")
          .enableHiveSupport()
          .getOrCreate()
      }

      //设置Checkpoint路径
      spark.sparkContext.setCheckpointDir("hdfs://hadoopMaster:9000/tmp/engine/sparkdemo/")

      //加载mysql数据 到rdd
      var rdd=spark.read.format("jdbc")
        .option("url", "jdbc:mysql://192.168.1.1:3306/datacenter")
        .option("user", "root")
        .option("password","test")
        .option("dbtable","score")
        .option("partitionColumn", "id")
        .option("lowerBound", "0")
        .option("upperBound", 5000000)
        .option("numPartitions", 1000)
        .load()

      rdd.persist()

      //引入隐式表达式
      import spark.sqlContext.implicits._

      /***计算每个学校的总分的学校排名***/
      //1.先计算总分
      var dataframe=rdd.groupBy("SCHOOL_NAME").agg(sum("TOTAL_SCORE").as("SCHOOL_MAX_SCORE"))

      //如果计算链很长
      //dataframe.persist()
      dataframe.checkpoint()
      //dataframe.show(10)


      //2.计算总分的排名
      dataframe=dataframe.withColumn("SCHOOL_MAX_SCORE_RANK",rank().over(Window.partitionBy().orderBy($"SCHOOL_MAX_SCORE".desc)))

      //保存计算结果
      val connectionProperties: Properties = new Properties
      connectionProperties.put("user", "root")
      connectionProperties.put("password", "test")
      connectionProperties.put("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      dataframe.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.1:3306/datacenter","sparkdemo1",connectionProperties)


      ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

      /***计算每个学校的最高分的学校排名 sql 方式***/
      //先计算最高分
      rdd.createOrReplaceTempView("temp_total_score")
      val df1=rdd.sqlContext.sql("select SCHOOL_NAME,sum(TOTAL_SCORE) SCHOOL_MAX_SCORE from temp_total_score group by SCHOOL_NAME")

      df1.createOrReplaceTempView("temp_total_score_max")
      val df2=df1.sqlContext.sql("select SCHOOL_NAME,SCHOOL_MAX_SCORE,rank() over(ORDER BY SCHOOL_MAX_SCORE DESC) as SCHOOL_MAX_SCORE_RANK from temp_total_score_max")

      //保存计算结果
      val connectionProperties1: Properties = new Properties
      connectionProperties1.put("user", "root")
      connectionProperties1.put("password", "test")
      connectionProperties1.put("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8;")
      df2.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.1:3306/datacenter","sparkdemo2",connectionProperties)

      spark.close()
    } catch {
      case e: Exception => {
        Logger.getLogger(SparkMain.getClass).error("数据计算分析异常", e)
        throw e
      }
    }
  }

}