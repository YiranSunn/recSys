package com.yiran.online

import redis.clients.jedis.Jedis
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.MongoClientURI
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies


//connection assistant, redis to mongoDB

object ConnHelper extends Serializable{

  lazy val jedis = new Jedis("localhost")

  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))

}


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

object onlineRecommender {

  val MAX_USER_RATINGS_NUM = 20

  val MAX_SIM_PRODUCTS_NUM = 20

  // table name

  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"

  val MONGODB_RATING_COLLECTION = "Rating"

  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(

      "spark.cores" -> "local[*]",

      "mongo.uri" -> "mongodb://localhost:27017/recommender",

      "mongo.db" -> "recommender",

      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(2))

    import spark.implicits._

    implicit val mongConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    //load data

    val simProductsMatrix = spark.read.option("uri", config("mongo.uri")).option("collection", MONGODB_PRODUCT_RECS_COLLECTION).format("com.mongodb.spark.sql").load().as[ProductRecs].rdd.map {

      recs => (recs.productId, recs.recs.map(x => (x.productId, x.score)).toMap)

    }.collectAsMap()

    //

    val simProductsMatrixBroadCast = sc.broadcast(simProductsMatrix)

    //kafka Configuration

    val kafkaPara = Map(

      "bootstrap.servers" -> "localhost:9092",

      "key.deserializer" -> classOf[StringDeserializer],

      "value.deserializer" -> classOf[StringDeserializer],

      "group.id" -> "recommender",

      "auto.offset.reset" -> "latest"

    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara))


    //Kafka stream ---> Rating stream

    val ratingStream = kafkaStream.map {

      case msg => var attr = msg.value().split("\\|")

        //four output features

        //userID, productID, score, timestamp

        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)

    }

    //recommendation algorithm

    ratingStream.foreachRDD {

      rdds =>
        rdds.foreach {

          case (userId, productId, score, timestamp) => println(">>>>>>>>>>>>>>>>")

            //convert data: user recent rating record -->into Array[productID, score]

            val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)

            //most similar products, ---> Array[productID]

            val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixBroadCast.value)

            //calculate recommendation level of each product ---> Array[productID, score]

            val streamRecs = computeProductScores(simProductsMatrixBroadCast.value, userRecentlyRatings, candidateProducts)

            //save in MongoDB

            saveDataToMongoDB(userId, streamRecs)

        }

    }

    ssc.start()

    println("start")

    ssc.awaitTermination()


  }


  import scala.collection.JavaConversions._


  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {

      jedis.lrange("userId:" + userId.toString, 0, num).map {

          item => val attr = item.split("\\:")

          (attr(0).trim.toInt, attr(1).trim.toDouble)


      }.toArray


  }


import com.mongodb.casbah.commons.MongoDBObject


  def getTopSimProducts(num:Int, productId:Int, userId:Int, simProducts:scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] ={

    //current similarity of profuct

    val allSimProducts = simProducts(productId).toArray

    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).find(MongoDBObject("userId" -> userId)).toArray.map{

        item => item.get("productId").toString.toInt

    }

    allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)

  }



  def computeProductScores(simProducts:scala.collection.Map[Int,scala.collection.immutable.Map[Int, Double]],

                           userRecentlyRatings:Array[(Int,Double)],

                           topSimProducts: Array[Int]): Array[(Int, Double)] ={

    //ArrayBuffer

    //[productID, score]

    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //productID, high score, count

    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //productID, low score, count

    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimProduct <- topSimProducts; userRecentlyRating <- userRecentlyRatings){

      val simScore = getProductsSimScore(topSimProduct, userRecentlyRating._1, simProducts)

      if(simScore >= 0.4){

        scores += ((topSimProduct, simScore * userRecentlyRating._2 ))

        if(userRecentlyRating._2 > 3){

          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct,0) + 1

        } else{

          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct,0) + 1

        }
      }

    }

    //calculate recommendation level

    //group by productID

    scores.groupBy(_._1).map{

      case (productId, scoreList) => (productId, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))

    }.toArray.sortWith(_._2>_._2)


  }



  def getProductsSimScore( product1:Int,

                           product2:Int,

                           simProducts:scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]

                          ): Double ={

        simProducts.get(product1) match {

        case Some(sim) => sim.get(product2) match {

        case Some(score) => score

        case None => 0.0


      }

        case None => 0.0

    }

  }



  def log(m:Int):Double ={

      math.log(m) / math.log(10)

  }


  def saveDataToMongoDB(userId:Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={

      val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

      //based on userID

      streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))

      streamRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))

  }


}



