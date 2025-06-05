package com.itbys.app

import java.util

import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

/**
  * Author Ari
  * Date 2025/6/5
  * Desc
  */
object _03_Maintain {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local").appName(this.getClass.getSimpleName).getOrCreate()

    // 配置catelog
    val catalog = new HiveCatalog()
    catalog.setConf(spark.sparkContext.hadoopConfiguration)

    val properties = new util.HashMap[String,String]()
    properties.put("warehouse", "hdfs://hadoop1:8020/warehouse/spark-iceberg")
    properties.put("uri", "thrift://hadoop1:9083")

    catalog.initialize("hive", properties)
    val table: Table = catalog.loadTable(TableIdentifier.of("db", "table1"))

    //1. 快照过期清理
    // 1天过期时间
    val tsToExpire: Long = System.currentTimeMillis() - (1000 * 60 * 60 * 24)
    table.expireSnapshots()
      .expireOlderThan(tsToExpire)
      .commit()

    //或者SparkActions可以并行运行大型表的表过期设置
    SparkActions.get()
      .expireSnapshots(table)
      .expireOlderThan(tsToExpire)
      .execute()

    //2. 删除无效文件
    SparkActions
      .get()
      .deleteOrphanFiles(table)
      .execute()

    //3. 合并小文件
    SparkActions
      .get()
      .rewriteDataFiles(table)
      .filter(Expressions.equal("category", "a"))
      .option("target-file-size-bytes", 1024L.toString) //1KB
      .execute()

  }

}
