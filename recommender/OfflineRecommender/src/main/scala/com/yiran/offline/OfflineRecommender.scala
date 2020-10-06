package com.yiran.offline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{Rating, ALS, MatrixFactorizationModel}
import org.jblas.DoubleMatrix

case class ProductRating(userId: Int,
                         productId: Int,
                         score: Double,
                         timestamp: Int
                        )


case class MongoConfig(uri: String,
                       db: String
                      )

case class Recommendation(productId: Int,
                          score: Double
                         )

case class UserRecs(userId: Int,
                    recs: Seq[Recommendation]
                   )

//product similarity
case class ProductRecs(productId: Int,
                       recs: Seq[Recommendation]
                      )

object OfflineRecommender {

       val MONGODB_RATING_COLLECTION = "Rating"

       val USER_RECS = "UserRecs"

       val PRODUCT_RECS = "ProductRecs"

       val USER_MAX_RECOMMENDATION = 20

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

         val ratingRDD = spark.read.option("uri",mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql").load().as[ProductRating].rdd.map(rating => (rating.userId, rating.productId, rating.score)).cache()

         //all user and product data

         val userRDD = ratingRDD.map(_._1).distinct()

         val productRDD = ratingRDD.map(_._2).distinct()

         //!!!Core part: train ALS model

         val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

         val (rank, iterations, lambda) = (5, 10, 0.01)

         val model = ALS.train(trainData, rank, iterations, lambda)

         //get prediction score matrix

         //userRDD * productRDD

         val userProducts = userRDD.cartesian(productRDD)

         val preRatings = model.predict(userProducts)

         //get recommendation from above results

         val userRecs = preRatings.filter(_.rating > 0).map(rating => (rating.user,(rating.product, rating.rating))).groupByKey().map{

             case (userId,recs) => UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)))

           }.toDF()

         userRecs.write.option("uri", mongoConfig.uri).option("collection", USER_RECS).mode("overwrite").format("com.mongodb.spark.sql").save()

         //table: similarity of products

         val productFeatures = model.productFeatures.map{

             case (productId, features) => (productId, new DoubleMatrix(features))

         }

         lazy val productRecs = productFeatures.cartesian(productFeatures).filter{

             case (a, b) => a._1 != b._1

           }.map{

             //cousinSim similarity

             case (a, b) => val simScore = cousinSim(a._2, b._2)

             (a._1, (b._1, simScore))

           }.filter(_._2._2 > 0.6).groupByKey().map{

             case (productId, recs) => ProductRecs(productId, recs.toList.map(x => Recommendation(x._1, x._2)))

            // case (productId, recs) => ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))


           }.toDF()

           productRecs.write.option("uri", mongoConfig.uri).option("collection", PRODUCT_RECS).mode("overwrite").format("com.mongodb.spark.sql").save()

           def cousinSim(product1: DoubleMatrix, product2:DoubleMatrix) : Double = {

             //formula

               product1.dot(product2) / (product1.norm2() * product2.norm2())

           }

           spark.stop()


       }

  }
