package com.yiran.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Rating( userId: Int,
                   productId: Int,
                   score: Double,
                   timestamp: Int
                 )


case class MongoConfig(uri: String,
                       db: String
                      )


object StatisticsRecommender {

  val MONGODB_RATING_COLLECTION = "Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

      val config = Map(

         "spark.cores" -> "local[*]",

         "mongo.uri" -> "mongodb://localhost:27017/recommender",

         "mongo.db" -> "recommender"
      )

      val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

      val spark = SparkSession.builder().config(sparkConf).getOrCreate()

      import spark.implicits._

      implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

      //load data

      val ratingDF = spark.read.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql").load().as[Rating].toDF()

      //temporary table
      ratingDF.createOrReplaceTempView("ratings")

      //Method1: historical popular items: based on ratings count

      val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count")

      storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

      //Method2: recent popular items, convert timestamp --> year/month

      import java.text._
      import java.util._

      val simpleDateFormat = new SimpleDateFormat("yyyyMM")

      //timestamp --> year+month

      spark.udf.register("changeDate",(x:Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

      //YearMonth

      val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")

      //create new table

      ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

      val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")

      storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

      //rateMoreRecentlyProducts.write.option("uri", mongoConfig.uri).option("collection",RATE_MORE_RECENTLY_PRODUCTS).mode("overwrite").format("com.mongodb.spark.sql").save()

      //Method3: recommend better items: average score of items

      val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")

      storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

      //averageProductsDF.write.option("uri",mongoConfig.uri).option("collection",AVERAGE_PRODUCTS).mode("overwrite").format("com.mongodb.spark.sql").save()

      spark.stop()

  }


      def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit={

        df.write.option("uri", mongoConfig.uri).option("collection", collection_name).mode("overwrite").format("com.mongodb.spark.sql").save()

      }

}
