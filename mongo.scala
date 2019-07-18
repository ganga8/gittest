package com.maven
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
//import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark._
//import scala.collection.JavaConverters._

import org.bson.BSONObject
import com.mongodb.hadoop.{ MongoInputFormat, MongoOutputFormat,BSONFileInputFormat, BSONFileOutputFormat}
import com.mongodb.hadoop.io.MongoUpdateWritable
object mongo extends App{
	// configuration for reading from mongodb
	val mongoConfig = new Configuration()
	mongoConfig.set("mongo.input.uri","mongodb://192.168.0.147:27020/bk_599c0aac8ac05_events.custom_events")
	val sparkConf = new SparkConf()
	val sc = new SparkContext("local","mongo", sparkConf)
	// RDD creation
	val rdd = sc.newAPIHadoopRDD(mongoConfig,classOf[MongoInputFormat],classOf[Object],classOf[BSONObject])
	val key_val_rdd = rdd.map(doc => ((doc._2.get("createdTime").toString,doc._2.get("subEvent").toString),1)).reduceByKey(_ + _)
	//val r = rdd.groupBy(rdd._2.get("date"))
	val documents = key_val_rdd.collect()
	for(i <- documents){
		println(i.get("user_id"))
	}
}
