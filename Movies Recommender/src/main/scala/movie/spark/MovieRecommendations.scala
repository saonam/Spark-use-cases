package movie.spark

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.io.{Codec, Source}


object MovieRecommendations {

  val spark= new SparkContext("local[*]","Movie recommendations")

  def mapUserIdAndMovieRatings(): RDD[(Int,(Int,Double))]= {
    val dataFile: RDD[String] =spark.textFile("files/u.data")

    val userIdMappedWithMovieIdAndRating: RDD[(Int, (Int, Double))] =dataFile.map(line =>{
      val fields=line.split("\\s+") //Using regex to split
      (fields(0).toInt,(fields(1).toInt,fields(2).toDouble))
    })
    userIdMappedWithMovieIdAndRating
  }

  def filterDuplicateMovieData(userIdAndPairOfMovies:(Int,((Int,Double),(Int,Double)))): Boolean = {
    val movieId1: Int =userIdAndPairOfMovies._2._1._1
    val movieId2: Int =userIdAndPairOfMovies._2._2._1
    movieId1<movieId2  //MovieId1==MovieId2 ( Same movie ) and MovieId2 < MovieId1 (Repeated Pair )
  }

  def mapMoviePairsWithRatings(userIdAndMovieData:(Int,((Int,Double),(Int,Double)))): ((Int,Int),(Double,Double))= {
    val movieId1=userIdAndMovieData._2._1._1
    val movieId2=userIdAndMovieData._2._2._1

    val rating1=userIdAndMovieData._2._1._2
    val rating2=userIdAndMovieData._2._2._2

    ((movieId1,movieId2),(rating1,rating2))
  }


  def computeCosineSimilarity(ratingPairs: Iterable[(Double, Double)]): (Double,Int) = {
    var numOfPairs: Int =0
    var sumXX: Double =0.0
    var sumYY: Double =0.0
    var sumXY: Double =0.0

    for(ratingPair: (Double, Double) <- ratingPairs){
      val ratingX: Double =ratingPair._1
      val ratingY: Double =ratingPair._2

      sumXX+=ratingX*ratingX
      sumYY+=ratingY*ratingY
      sumXY+=ratingX*ratingY
      numOfPairs+=1
    }

    val numerator: Double =sumXY
    val denominator: Double =Math.sqrt(sumXX)*Math.sqrt(sumYY)
    val result: Double =numerator/denominator
    (result,numOfPairs)
  }


  def mapMovieIdAndName():Map[Int,String]={

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val idAndNameMapped: Map[Int, String] =Source.fromFile("files/u.item").getLines().map(line =>{
      val lineArr=line.split('|')
      (lineArr.head.toInt,lineArr(1))
    }).toMap[Int,String]

    idAndNameMapped
  }

  def suggestTop10Movies(moviesAndSimilarityScore: RDD[((Int, Int), (Double, Int))],args:Array[String]): Unit = {
    println("Loading movie names: ")

    val scoreThreshold: Double = 0.97  //Calculated according to our data set
    val coOccurenceThreshold: Double = 50.0 //Calculated according to our data set

    val movieId: Int =args.head.toInt

    val moviePairsFilteredAccordingToThreshold: RDD[((Int, Int), (Double, Int))] =moviesAndSimilarityScore.filter((moviePairAndScore: ((Int, Int), (Double, Int))) => {
      val moviePair: (Int, Int) =moviePairAndScore._1
      val ratingAndNumOfPairs: (Double, Int) =moviePairAndScore._2
      (moviePair._1==movieId || moviePair._2==movieId) && ratingAndNumOfPairs._1>scoreThreshold && ratingAndNumOfPairs._2>coOccurenceThreshold
    })

    val first10MoviesAndTheirScores: Array[((Int, Int), (Double, Int))] =moviePairsFilteredAccordingToThreshold.take(10)

    val idAndMovieNames=mapMovieIdAndName()

    println("Top 10 movies related to "+idAndMovieNames(movieId)+" are:")
    first10MoviesAndTheirScores.foreach(moviePairAndScore =>{
      val movie1: Int =moviePairAndScore._1._1
      val movie2: Int =moviePairAndScore._1._2
      var suggestedMovie: Int =movie2
      if(movie2==movieId) {
        suggestedMovie = movie1
      }

      println(idAndMovieNames(suggestedMovie))
    })
  }

  def main(args: Array[String]): Unit = {

    //Setting Logs level to Error only
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Mapping user Id with the movie Id and the rating
    val userIdMappedWithMovieIdAndRatings: RDD[(Int, (Int, Double))] =mapUserIdAndMovieRatings()

    //Finding movie sets along with the rating given for each particular user
    val pairOfMoviesWatchedBySameUser: RDD[(Int, ((Int, Double), (Int, Double)))] =userIdMappedWithMovieIdAndRatings.join(userIdMappedWithMovieIdAndRatings)

    //Filtering duplicate movies data
    val pairOfMoviesWithoutDuplicates: RDD[(Int, ((Int, Double), (Int, Double)))] =pairOfMoviesWatchedBySameUser.filter(filterDuplicateMovieData)

    //Mapping userMovieData in the form (movie1,movie2) => (rating1,rating2)
    val moviePairAndRatings: RDD[((Int, Int), (Double, Double))] =pairOfMoviesWithoutDuplicates.map(mapMoviePairsWithRatings)

    //Combining all the same movie sets with their ratings
    val groupOfRatingPairsForSameMoviePair: RDD[((Int, Int), Iterable[(Double, Double)])] =moviePairAndRatings.groupByKey()

    //Computing similarity among rating pairs using Cosine similarity algorithm
    val moviePairsAndSimilarityScore: RDD[((Int, Int), (Double, Int))] =groupOfRatingPairsForSameMoviePair.mapValues(computeCosineSimilarity)

    //Similar movies will be printed for the movie which we will pass as command line argument
    if(args.length>0)
    suggestTop10Movies(moviePairsAndSimilarityScore,args)

    else
      println("Enter movie Id as command line argument")


  }

}
