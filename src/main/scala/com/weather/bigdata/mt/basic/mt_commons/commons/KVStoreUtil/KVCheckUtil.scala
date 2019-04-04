package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.SetOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.sparkUtil.ContextUtil
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

import scala.collection.mutable

object KVCheckUtil {
  val hostNameKey=KVStoreConf.hostNameKey
  val portKey=KVStoreConf.portKey
  val authKey=KVStoreConf.authKey
  val ChangeRefKey=KVStoreConf.ChangeRefKey

  def main (args: Array[String]): Unit = {
    val metaurl: String = {

      "http://220.243.129.242:8700/weatherstore/v1/api/metaapi"

    }
    val type0: String = args(0)
    println("type=" + type0)
    this.KVcheck(metaurl, type0)
  }

  //未开发完
  private def KVcheck(metaurl:String,`type`:String,timeout:Int=30000): Unit ={
    val type_lower=`type`.toLowerCase
    val typea_upper=`type`.toUpperCase

    val sc=ContextUtil.getSparkContext()
    val hostJson:JSONObject=KVStoreConf.gethostJson(metaurl,typea_upper)
    val dbSet:mutable.HashSet[String]=SetOperation.Set2scalaSet(hostJson.keySet())
    val dbArr=dbSet.toArray
    val dbRdd:RDD[String]=sc.parallelize(dbArr,dbSet.size)

    dbRdd.foreach(dbNo=>{
      val ip=hostJson.getJSONObject(dbNo).getString(this.hostNameKey)
      val port=hostJson.getJSONObject(dbNo).getIntValue(this.portKey)
      val auth=hostJson.getJSONObject(dbNo).getString(this.authKey)
      val jedis = new Jedis(ip, port, timeout)
      jedis.auth(auth)

      val timeStep=KVStoreConf.type2timeStep(typea_upper)
      val kSet=SetOperation.Set2scalaSet(jedis.keys("*"))
      //      val obsHours:mutable.HashSet[((Double,Double),Int)]=kSet.filter(s=>s.startsWith(type_lower)).map(
      //        k=>{
      //          val ss:Array[String]=k.split("_")
      //          val type1:String=ss(0)
      //          val latIndex:Int=ss(1).toInt
      //          val lonIndex:Int=ss(2).toInt
      //          val lat:Double=KVStoreConf.latIndex2lat(latIndex)
      //          val lon:Double=KVStoreConf.lonIndex2lon(lonIndex)
      //
      //          val changeRefStr:String=jedis.hgetAll(k).get(this.ChangeRefKey)
      //          val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(changeRefStr,timeStep))
      //          val obsdate:Date=DataFormatUtil.YYYYMMDDHHMMSS0(olddateStr)
      //          val obsHour:Int=obsdate.getHours
      //          //        println("lat="+lat+";lon="+lon)
      //          ((lat,lon),obsHour)
      //        }
      //      )
      kSet.filter(s=>s.startsWith(type_lower)).foreach(
        k=>{
          val ss:Array[String]=k.split("_")
          val type1:String=ss(0)
          val latIndex:Int=ss(1).toInt
          val lonIndex:Int=ss(2).toInt
          val lat:Double=KVStoreConf.latIndex2lat(latIndex)
          val lon:Double=KVStoreConf.lonIndex2lon(lonIndex)

          val changeRefStr:String=jedis.hgetAll(k).get(this.ChangeRefKey)
          val olddateStr: String = DateFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(changeRefStr, timeStep))
          val obsdate: Date = DateFormatUtil.YYYYMMDDHHMMSS0(olddateStr)
          val obsHour:Int=obsdate.getHours
          //        println("lat="+lat+";lon="+lon)
          println(((lat,lon),obsHour))
        }
      )

      jedis.close()
      //      obsHours.foreach(obsHour=>println(obsHour))
    })
  }
}
