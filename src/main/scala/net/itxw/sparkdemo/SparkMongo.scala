package net.itxw.sparkdemo

import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Encoders, Row, SaveMode, SparkSession}

import scala.collection.mutable

object SparkMongo {

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
          //.enableHiveSupport()
          .getOrCreate()
      } else {
        SparkSession.builder().appName("spark-demo")
          //.config("spark.some.config.option", "some-value")
          .enableHiveSupport()
          .getOrCreate()
      }

      var projectRdd=spark.read.format("com.mongodb.spark.sql")
        .option("spark.mongodb.input.uri", "mongodb://10.10.22.209:23000/project_data.project_list")
        .option("spark.mongodb.input.partitioner", "MongoShardedPartitioner")
        .option("spark.mongodb.input.partitionerOptions.shardkey","_id")
        .load()


//    方法1
      projectRdd.createOrReplaceTempView("project_list_tmp")
      var schoolRdd=spark.sql("select project projectId,name projectName,explode(schools) schools from project_list_tmp")
      schoolRdd.createOrReplaceTempView("school_list_tmp")
      schoolRdd=spark.sql("select projectId,projectName,schools.school schoolId,schools.name schoolName from school_list_tmp")

      //方法3
//      var schoolRdd=projectRdd.select(projectRdd("project").as("projectId"),projectRdd("name").as("projectName"),explode(projectRdd("schools")).as("school"))
//      schoolRdd=schoolRdd.select(schoolRdd("projectId"),schoolRdd("projectName"),schoolRdd("school")("school").as("schoolId"),schoolRdd("school")("name").as("schoolName"))

      //方法3
//      var schoolRdd = projectRdd.mapPartitions(rows=>{
//        rows.map(row=>{
//          var schoolRows=row.getAs[mutable.WrappedArray[Row]]("schools")
//          schoolRows.map(schoolRow=>{
//            (row.getAs[String]("project"),row.getAs[String]("name"),schoolRow.getAs[String]("school"),schoolRow.getAs[String]("name"))
//          })
//        }).flatten
//      })(Encoders.tuple(Encoders.STRING,Encoders.STRING,Encoders.STRING,Encoders.STRING)).toDF("projectId","projectName","schoolId","schoolName")

      spark.close()
    } catch {
      case e: Exception => {
        Logger.getLogger(SparkMongo.getClass).error("数据计算分析异常", e)
        throw e
      }
    }
  }

}