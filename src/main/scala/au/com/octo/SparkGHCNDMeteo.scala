package au.com.octo

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import utils.MeteoUtils


object SparkGHCNDMeteo {

  private val config =  ConfigFactory.load()

	def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkMeteo").setMaster("local[*]")
		val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val confFiles = config.getConfig("files")
    val confGhcnd = confFiles.getConfig("ghcnd")

    val fileGhcnd = confGhcnd.getString("fileStations")
    val file2013 = confGhcnd.getString("file2013")
    val file2014 = confGhcnd.getString("file2014")

    val ghcndDF: DataFrame = getStationDataframe(sqlContext, fileGhcnd)
    val weatherData: DataFrame = getWeatherDataframe(sqlContext, file2013)


    val closestStations = SparkMeteo.getClosestStations(5, -33.923221, 151.173230, ghcndDF)
    //val result = getDataForStations("PRCP", "20140507", closestStations, weatherData)
    val result = SparkMeteo.getAllDaysDataForStations("PRCP", closestStations, weatherData)
    closestStations.foreach(println)
    result.foreach(t => {
      println(t._1+" "+t._2)
    })

		System.exit(0)
	}

  def getWeatherDataframe(sqlContext: SQLContext, file:String): DataFrame = {

    val schema = StructType(SparkMeteo.dataHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val weatherRaw = sqlContext.sparkContext.textFile(file)
      .map(l => l.split(",", -1))
      .map(spl => Row(spl(0), spl(1), spl(2), spl(3)))
    val df = sqlContext.createDataFrame(weatherRaw, schema)
    df.withColumn("v", MeteoUtils.toDouble(df("v")))
  }

  def getStationDataframe(sqlContext: SQLContext, file:String): DataFrame = {

    val schema = StructType(SparkMeteo.stationsHeader.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val stationRaw = sqlContext.sparkContext.textFile(file)
      .map(l => SparkMeteo.trimHeaderRow(getHeaderRow(l)))
    val df = sqlContext.createDataFrame(stationRaw, schema)
    df.withColumn("lat", MeteoUtils.toDouble(df("lat")))
      .withColumn("long", MeteoUtils.toDouble(df("long")))
  }

  def getHeaderRow(l: String): Row = {
    Row(l.substring(0, 11), l.substring(11, 21), l.substring(21, 31))
  }
}



