package com.yiran.offline

import com.yiran.offline.OfflineRecommender.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import breeze.numerics.sqrt


//evaluation model

object ALSTrainer {


  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    //load data

    val ratingRDD = spark.read.option("uri",mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql").load().as[ProductRating].rdd.map(rating => Rating(rating.userId, rating.productId, rating.score)).cache()

    //split data: train data, test data

    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))

    val trainingRDD = splits(0)

    val testingRDD = splits(1)

    adjustALSParams(trainingRDD, testingRDD)

    spark.close()

  }


  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={

    val result = for(rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01))

      yield {

        val model = ALS.train(trainData, rank,10, lambda)

        val rmse = getRMSE(model, testData)

        (rank, lambda, rmse)

      }

    println(result.minBy(_._3))

  }


  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]):Double = {

    //prediction score matrix

    val userProducts = data.map(item => (item.user, item.product))

    val predictRating = model.predict(userProducts)

    val real = data.map(item => ((item.user, item.product), item.rating))

    val predict = predictRating.map(item => ((item.user, item.product), item.rating))

    sqrt(

        real.join(predict).map{

          case((userId, productId), (real, pre))=>

          val err = real - pre

          err * err

        }.mean()

    )
  }

}
