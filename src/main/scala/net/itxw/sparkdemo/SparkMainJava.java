package com.ajia.sparkdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;

import java.util.Properties;

/**
 * @Description:
 * @Author: houyong
 * @Date: 2020/4/8
 */
public class SparkMainJava {
    public static void main(String[] args) {
        try {
            //获取参数 开发模式
            boolean isDev =args.length >= 1&&args[0] == "dev"?true:false;

            //初始化spark
            SparkSession spark = null;
            if (isDev) {
                System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.3");

                spark=SparkSession.builder().appName("Sparkdemo")
                        //.config("spark.some.config.option", "some-value")
                        .config("spark.debug.maxToStringFields", "100")
                        .master("local[*]")
                        .enableHiveSupport()
                        .getOrCreate();
            } else {
                spark=SparkSession.builder().appName("spark-demo")
                        //.config("spark.some.config.option", "some-value")
                        .enableHiveSupport()
                        .getOrCreate();
            }

            //设置Checkpoint路径
            spark.sparkContext().setCheckpointDir("hdfs://hadoopMaster:9000/tmp/engine/sparkdemo/");

            //加载mysql数据 到rdd
            Dataset rdd=spark.read().format("jdbc")
                    .option("url", "jdbc:mysql://192.168.1.1:3306/datacenter")
                    .option("user", "root")
                    .option("password","test")
                    .option("dbtable","subject_total_score")
                    .option("partitionColumn", "id")
                    .option("lowerBound", "0")
                    .option("upperBound", 5000000)
                    .option("numPartitions", 1000)
                    .load();

            rdd.persist();

            /***计算每个学校的最高分的学校排名***/
            //1.先计算总分
            Dataset dataframe=rdd.groupBy("SCHOOL_NAME").agg(functions.sum("TOTAL_SCORE").as("SCHOOL_MAX_SCORE"));

            //如果计算链很长
            //dataframe.persist()
            //dataframe.checkpoint()
            //dataframe.show(10)


            //2.计算总分的排名
            dataframe=dataframe.withColumn("SCHOOL_MAX_SCORE_RANK",functions.rank().over(Window.partitionBy().orderBy(functions.col("SCHOOL_MAX_SCORE").desc())));

            //保存计算结果
            Properties connectionProperties = new Properties();
            connectionProperties.put("user", "root");
            connectionProperties.put("password", "test");
            connectionProperties.put("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            dataframe.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.1:3306/datacenter","sparkdemo1",connectionProperties);


            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            /***计算每个学校的最高分的学校排名 sql 方式***/
            //先计算最高分
            rdd.createOrReplaceTempView("temp_total_score");
            Dataset df1=rdd.sqlContext().sql("select SCHOOL_NAME,sum(TOTAL_SCORE) SCHOOL_MAX_SCORE from temp_total_score group by SCHOOL_NAME");

            df1.createOrReplaceTempView("temp_total_score_max");
            Dataset df2=df1.sqlContext().sql("select SCHOOL_NAME,SCHOOL_MAX_SCORE,rank() over(ORDER BY SCHOOL_MAX_SCORE DESC) as SCHOOL_MAX_SCORE_RANK from temp_total_score_max");

            //保存计算结果
            Properties connectionProperties1 = new Properties();
            connectionProperties1.put("user", "root");
            connectionProperties1.put("password", "test");
            connectionProperties1.put("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8;");
            df2.write().mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.1.1:3306/datacenter","sparkdemo2",connectionProperties);

            spark.close();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}
