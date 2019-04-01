package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

private object AllPoint {
  def main(args:Array[String]): Unit ={
    val sc = ContextUtil.getSparkContext( )

    val jsonFile0: String = "D:/Data/forecast/Input/region0.txt"
    val jsonFile1: String = "D:/Data/forecast/Input/region1.txt"
    val jsonFile2: String = "D:/Data/forecast/Input/region2.txt"
    val jsonFile3: String = "D:/Data/forecast/Input/region3.txt"
    val jsonFile4: String = "D:/Data/forecast/Input/region4.txt"

    val jsonArr0: Array[JSONObject] = ReadFile.ReadJsonArr(jsonFile0)
    val jsonArr1: Array[JSONObject] = ReadFile.ReadJsonArr(jsonFile1)
    val jsonArr2: Array[JSONObject] = ReadFile.ReadJsonArr(jsonFile2)
    val jsonArr3: Array[JSONObject] = ReadFile.ReadJsonArr(jsonFile3)
    val jsonArr4: Array[JSONObject] = ReadFile.ReadJsonArr(jsonFile4)

    //    val jsonArr:Array[JSONObject]=jsonArr1.union(jsonArr2).union(jsonArr3).union(jsonArr4)
    val jsonArr: Array[JSONObject] = jsonArr0
    val splitNum = jsonArr.length
    println("splitNum=" + splitNum)
    val jsonRdd = sc.parallelize(jsonArr, splitNum)

    val pointSets: RDD[(mutable.HashSet[String], mutable.HashSet[String])] = jsonRdd.map(
      jsonStr=>{
        //        val AO:ArrayOperation=new ArrayOperation
        val (idName: String, timeBegin, timeEnd, timeStep, heightBegin, heightEnd, heightStep, latBegin, latEnd, latStep, lonBegin, lonEnd, lonStep) = Analysis.analysisJsonProperties(jsonStr)
        val lats: Array[Double] = ArrayOperation.ArithmeticArray(latBegin, latEnd, 0.01d)
        val lons: Array[Double] = ArrayOperation.ArithmeticArray(lonBegin, lonEnd, 0.01d)

        val set:mutable.HashSet[String]=new mutable.HashSet[String]
        val set2: mutable.HashSet[String] = new mutable.HashSet[String]
        lats.foreach(
          lat=>{
            lons.foreach(
              lon=>{
                val latStr: String = NumOperation.K2dec(lat).toString
                val lonStr: String = NumOperation.K2dec(lon).toString
                val latlonStr:String=latStr+","+lonStr
                if (set.contains(latlonStr)) {
                  set2.add(latlonStr)
                } else {
                  set.add(latlonStr)
                }
              }
            )
          }
        )
        println("idName="+idName+",size="+set.size)
        (set, set2)
      }
    )
    val pointSet: (mutable.HashSet[String], mutable.HashSet[String]) = pointSets.reduce((x, y) => {
      val x0 = x._1.union(y._1)
      val y0 = x._2.union(y._2)
      (x0, y0)
    })

    println("Allpoints=" + pointSet._1.size)
    println("2points=" + pointSet._2.size)
    println("splitNum=" + splitNum)
  }
}
