package com.yiran.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


//product dataset


case class Product(productId: Int,
                   name: String,
                   imageUrl: String,
                   categories: String,
                   tags: String
                  )

//ratings dataset

case class Rating( userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                  )


case class MongoConfig(uri: String,
                       db: String
                      )


object dataload {

  val PRODUCT_DATA_PATH = "/Users/yiransun/Desktop/bigdata/recSys/recommender/dataload/src/main/resources/products.csv"

  val RATING_DATA_PATH = "/Users/yiransun/Desktop/bigdata/recSys/recommender/dataload/src/main/resources/ratings.csv"

  val MONGODB_PRODUCT_COLLECTION = "Product"

  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {

      val config = Map(

        "spark.cores" -> "local[*]",
        "mongo.uri" -> "mongodb://localhost:27017/recommender",
        "mongo.db" -> "recommender"

      )

      //spark config

      val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("dataload")

      //spark session

      val spark = SparkSession.builder().config(sparkConf).getOrCreate()

      import spark.implicits._

      val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)

      //in order to fit mongoDB

      val productDF = productRDD.map(item => {

        val attr = item.split("\\^")

        Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)

      }).toDF()

      val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

      val ratingDF = ratingRDD.map(item => {

        val attr = item.split(",")

        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)

      }).toDF()

      implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

      storeDataInMongoDB(productDF, ratingDF)

      spark.stop()

  }

  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

      //url

      val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

      val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)

      val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

      //delete if exist

      productCollection.dropCollection()

      ratingCollection.dropCollection()

      //store data in table

      productDF.write.option("uri", mongoConfig.uri).option("collection", MONGODB_PRODUCT_COLLECTION).mode("overwrite").format("com.mongodb.spark.sql").save()

      ratingDF.write.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).mode("overwrite").format("com.mongodb.spark.sql").save()

      //index

      productCollection.createIndex(MongoDBObject("productId" -> 1))

      ratingCollection.createIndex(MongoDBObject("productId" -> 1))

      ratingCollection.createIndex(MongoDBObject("userId" -> 1))

      mongoClient.close()


  }



}
