package utils

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

object MeteoUtils {

  val csvDBFormat: String = "com.databricks.spark.csv"

  val toDouble = udf[Double, String](d => if(d.trim.isEmpty) Double.NaN else  d.trim.toDouble)

  def csvToDF(sqlc: SQLContext, stationsRawIn: String): DataFrame = {
    sqlc.read.format(csvDBFormat).option("header", "true").load(stationsRawIn)
  }

  def DFtoCsv(df:DataFrame, fileOut:String) = {
    df.write.format(csvDBFormat).option("header", "true").save(fileOut)
  }

  def spatialPonderation(u: Double): Double = {
    1 / Math.pow(1 + u, 2)
  }




}
