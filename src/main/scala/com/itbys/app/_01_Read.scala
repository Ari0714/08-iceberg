package com.itbys.app

import org.apache.spark.sql.SparkSession

/**
  * Author Ari
  * Date 2025/6/5
  * Desc 
  */
object _01_Read {

  def main(args: Array[String]): Unit = {

    /**
      * first：加入iceberg spark runtime
      */

    // 配置catelog
    val spark: SparkSession = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      //指定hive catalog, catalog名称为iceberg_hive
      .config("spark.sql.catalog.iceberg_hive", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_hive.type", "hive")
      .config("spark.sql.catalog.iceberg_hive.uri", "thrift://hadoop1:9083")
      //    .config("iceberg.engine.hive.enabled", "true")
      //指定hadoop catalog，catalog名称为iceberg_hadoop
      .config("spark.sql.catalog.iceberg_hadoop", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_hadoop.type", "hadoop")
      .config("spark.sql.catalog.iceberg_hadoop.warehouse", "hdfs://hadoop1:8020/warehouse/spark-iceberg")
      .getOrCreate()


    //读取
    spark.read
      .format("iceberg")
      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a")
      .show()

    //时间旅行，时间、快照id
    spark.read
      .option("as-of-timestamp", "499162860000")
      .format("iceberg")
      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a")
      .show()
    spark.read
      .option("snapshot-id", 7601163594701794741L)
      .format("iceberg")
      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a")
      .show()

    //增量查询
    spark.read
      .format("iceberg")
      .option("start-snapshot-id", "10963874102873")
      .option("end-snapshot-id", "63874143573109")
      .load("hdfs://hadoop1:8020/warehouse/spark-iceberg/default/a")
      .show()

  }

}
