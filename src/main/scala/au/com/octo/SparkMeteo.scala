package au.com.octo

import org.apache.spark.sql.{DataFrame, Row}
import utils.MeteoUtils


object SparkMeteo {

  val stationsHeader: String = "id lat long"
  val dataHeader: String = "id date type v"

  def trimHeaderRow(r:Row): Row = {
    Row(r.getString(0).trim, r.getString(1).trim, r.getString(2).trim)
  }

  def getClosestStations(nb:Int, lat:Double, long:Double, stations:DataFrame): Map[String, Double] = {
    stations
      .map(r => (Math.pow(r.getAs[Double]("lat")-lat, 2) + Math.pow(r.getAs[Double]("long")-long, 2), r.getAs[String]("id")))
      .takeOrdered(nb)(Ordering.by[(Double, String), Double](_._1))
      .map(v => v._2 -> v._1).toMap
  }

  def getDataForStations(tp:String, dt:String, stationsFilt:Map[String, Double],
                             weatherData:DataFrame) :Double = {
    // dt format YYYYMMdd
    val tuple = weatherData
      .filter(weatherData("id").isin(stationsFilt.keySet.toList:_*))
      .filter(weatherData("type").equalTo(tp).and(weatherData("date").equalTo(dt)))
      .map(getVal(stationsFilt, _))
      .filter(!_._1.isNaN)
      .map(v => (v._1*v._2, v._2))
      .reduce((u,v) => (u._1+v._1, u._2+v._2))

    return tuple._1/tuple._2
  }

  def getAllDaysDataForStations(tp:String, stationsFilt:Map[String, Double],
                         weatherData:DataFrame) :Array[(String, Double)] = {
    weatherData
      .filter(weatherData("id").isin(stationsFilt.keySet.toList:_*))
      .filter(weatherData("type").equalTo(tp))
      .map(r => (r.getAs[String]("date"), getVal(stationsFilt, r)))
      .filter(!_._2._1.isNaN)
      .map(v => (v._1, (v._2._1*v._2._2, v._2._2)))
      .reduceByKey((u,v) => (u._1+v._1, u._2+v._2))
      .map(a => (a._1, a._2._1/a._2._2))
      .collect()
  }

  def getVal(stationsFilt: Map[String, Double], r: Row): (Double, Double) = {
    (r.getAs[Double]("v"), MeteoUtils.spatialPonderation(stationsFilt.get(r.getAs[String]("id")).get))
  }

}



