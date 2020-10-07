package com.yiran.itemCF

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/*

 * @author Daisy Suen

 * @Date 11:56 AM

 */


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

//product similarity
case class ProductRecs(productId: Int,
                       recs: Seq[Recommendation]
                      )

object itemCFRecommender {


       val MONGODB_RATING_COLLECTION = "Rating"

       val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"

       val MAX_RECOMMENDATION = 10


  def main(args: Array[String]): Unit = {

    val config = Map(

      "spark.cores" -> "local[*]",

      "mongo.uri" -> "mongodb://localhost:27017/recommender",

      "mongo.db" -> "recommender"

    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    //load data

    val ratingDF = spark.read.option("uri", mongoConfig.uri).option("collection", MONGODB_RATING_COLLECTION).format("com.mongodb.spark.sql").load().as[ProductRating].map(x => (x.userId, x.productId, x.score)).toDF("userId", "productId", "score")

    //algorithm:

    //rating number of each product, order by productID

    val productRatingCountDF = ratingDF.groupBy("productId").count()

    //add to table

    val ratingWithCountDF = ratingDF.join(productRatingCountDF, "productId")

    //pair rating using userID

    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId").toDF("userId", "product1", "score1", "count1", "product2", "score2", "count2").select("userId", "product1", "count1", "product2", "count2")

    joinedDF.createOrReplaceTempView("joined")

    val cooccurrenceDF = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
      """.stripMargin

    ).cache()

    lazy val simDF = cooccurrenceDF.map{

                row => val coocSim = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))

                (row.getInt(0), (row.getInt(1), coocSim))

                                  }.rdd.groupByKey().map{

                                        case (productId, recs) => ProductRecs(productId, recs.toList.filter(x => x._1 != productId).sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)).take(MAX_RECOMMENDATION))

                                                        }.toDF()

    //save to mongoDB

    simDF.write.option("uri", mongoConfig.uri).option("collection", ITEM_CF_PRODUCT_RECS).mode("overwrite").format("com.mongodb.spark.sql").save()


    def cooccurrenceSim(coCount: Long, count1: Long, count2: Long): Double ={

                                      coCount / math.sqrt( count1 * count2)

                            }

    spark.stop()



  }

  }

























