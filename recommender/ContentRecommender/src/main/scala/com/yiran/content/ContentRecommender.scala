package com.yiran.content


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.linalg.SparseVector
import org.jblas.DoubleMatrix


case class Product(productId: Int,
                   name: String,
                   imageUrl: String,
                   categories: String,
                   tags: String
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



object ContentRecommender {

       val MONGODB_PRODUCT_COLLECTION = "Product"

       val CONTENT_PRODUCT_RECS = "ProductRecsbasedContent"

       def main(args: Array[String]): Unit = {

           val config = Map(

               "spark.cores" -> "local[*]",

               "mongo.uri" -> "mongodb://localhost:27017/recommender",

               "mongo.db" -> "recommender"

           )


         val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")

         val spark = SparkSession.builder().config(sparkConf).getOrCreate()

         import spark.implicits._

         implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

         //load data

         val productTagsDF = spark.read.option("uri",mongoConfig.uri).option("collection", MONGODB_PRODUCT_COLLECTION).format("com.mongodb.spark.sql").load().as[Product].map(x => (x.productId, x.name, x.tags.map(c => if(c == '|') ' ' else c))).toDF("productId", "name", "tags").cache()

         //Algorithm:

         val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")

         val wordsDataDF = tokenizer.transform(productTagsDF)

         val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(300)

         val featurizedDataDF = hashingTF.transform(wordsDataDF)

         val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

         //train idf model

         val idfModel = idf.fit(featurizedDataDF)

         val rescaledDataDF = idfModel.transform(featurizedDataDF)

         val productFeatures = rescaledDataDF.map{

            row => ( row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)

         }.rdd.map{

           case(productId, features) => (productId, new DoubleMatrix(features))

         }

         //cosine

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

         productRecs.write.option("uri", mongoConfig.uri).option("collection", CONTENT_PRODUCT_RECS).mode("overwrite").format("com.mongodb.spark.sql").save()

         def cousinSim(product1: DoubleMatrix, product2:DoubleMatrix) : Double = {

           //formula

           product1.dot(product2) / (product1.norm2() * product2.norm2())

         }

         spark.stop()


     }


}

