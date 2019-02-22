package com.thk7

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}


object MovieRecommendations {

  val spark= new SparkContext("local[*]","Movie recommendations")

  def mapUserIdAndMovieRatings(): RDD[(Int,(Int,Double))]= {
    val fileRdd=spark.textFile("files/u.data")

    val mappedElements=fileRdd.map(line =>{
      val words=line.split("\\s+")
      (words(0).toInt,(words(1).toInt,words(2).toDouble))
    })
    mappedElements
  }

  def filterDuplicateMovieData(userIdAndMovieData:(Int,((Int,Double),(Int,Double)))): Boolean = {
    val movieId1=userIdAndMovieData._2._1._1
    val movieId2=userIdAndMovieData._2._2._1
    movieId1<movieId2
  }

  def mapMoviesWise(userIdAndMovieData:(Int,((Int,Double),(Int,Double)))): ((Int,Int),(Double,Double))= {
    val movieId1=userIdAndMovieData._2._1._1
    val movieId2=userIdAndMovieData._2._2._1

    val rating1=userIdAndMovieData._2._1._2
    val rating2=userIdAndMovieData._2._2._2

    ((movieId1,movieId2),(rating1,rating2))
  }


  def computeCosineSimilarity(ratings: Iterable[(Double, Double)]): (Double,Int) = {
    var numOfPairs=0
    var sumXX=0.0
    var sumYY=0.0
    var sumXY=0.0

    for(rating <- ratings){
      val ratingX=rating._1
      val ratingY=rating._2

      sumXX+=ratingX*ratingX
      sumYY+=ratingY*ratingY
      sumXY+=ratingX*ratingY
      numOfPairs+=1
    }

    val numerator=sumXY
    val denominator=Math.sqrt(sumXX)*Math.sqrt(sumYY)
    val result=numerator/denominator

    (result,numOfPairs)
  }


  def mapMovieIdAndName():Map[Int,String]={

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val idAndNameMapped=Source.fromFile("files/u.item").getLines().map(line =>{
      val lineArr=line.split('|')
      (lineArr.head.toInt,lineArr(1))
    }).toMap[Int,String]

    idAndNameMapped
  }

  def suggestTop10Movies(moviesAndSimilarityScore: RDD[((Int, Int), (Double, Int))],args:Array[String]): Unit = {
    println("Loading movie names: ")

    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0

    val movieId=args.head.toInt

    val similarMoviePairsFiltered=moviesAndSimilarityScore.filter(movieAndScore => {
      val moviePair=movieAndScore._1
      val simScore=movieAndScore._2
      (moviePair._1==movieId || moviePair._2==movieId) && simScore._1>scoreThreshold && simScore._2>coOccurenceThreshold
    })

    val first10Pairs=similarMoviePairsFiltered.take(10)

    val idAndNameMapped=mapMovieIdAndName()

    println("Top 10 movies related to "+idAndNameMapped(movieId)+" are:")
    first10Pairs.foreach(moviePairAndScore =>{
      val movie1=moviePairAndScore._1._1
      val movie2=moviePairAndScore._1._2
      var suggestedMovie=movie2
      if(movie2==movieId)
        suggestedMovie=movie1

      println(idAndNameMapped(suggestedMovie))
    })
  }

  def main(args: Array[String]): Unit = {

    //Setting Logs level to Error only
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Mapping user Id with the movie Id and the rating
    val userIdAndRatings=mapUserIdAndMovieRatings()

    //Finding movie sets along with the rating given for each particular user
    val ratingsJoined=userIdAndRatings.join(userIdAndRatings)

    //Filtering duplicate movies data
    val userMovieMappingFiltered=ratingsJoined.filter(filterDuplicateMovieData)

    //Mapping userMovieData in the form (movie1,movie2) => (rating1,rating2)
    val userAndMovieWiseMapping=userMovieMappingFiltered.map(mapMoviesWise)

    //Combining all the same movie sets with their ratings
    val movieSetAndRatings=userAndMovieWiseMapping.groupByKey()

    //Computing similarity among them using Cosine similarity algorithm
    val moviesAndSimilarityScore=movieSetAndRatings.mapValues(computeCosineSimilarity)

    if(args.length>0)
    suggestTop10Movies(moviesAndSimilarityScore,args)

    else
      println("Enter movie Id as command line argument")


  }

}
