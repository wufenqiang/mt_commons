package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import java.io.InputStream

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.workersUtil

import scala.collection.mutable

object IdNameMap {
  private val eachpartition:Int={
    try{
      val prop:java.util.Properties={
        val prop = new java.util.Properties()
        val loader = Thread.currentThread.getContextClassLoader()
        val loadfile: InputStream=loader.getResourceAsStream("config.properties")
        prop.load(loadfile)
        prop
      }
      prop.getProperty("sparksubmit_config_out.properties").toInt
    }catch{
      case e:Exception=>{
        val e0=new Exception(e+";加载sparksubmit_config_out.properties 失败.查看任务上传,eachpartition取默认5")
        PropertiesUtil.log.warn(e0)
        5
      }
    }

  }
  def partitionCoefficient(splitNum:Int):Int=(splitNum/eachpartition)+1

  def getidNameMap(timeSteps: Array[Double], jsonFile: String): mutable.HashMap[String, Array[(String, Double)]] = {
    val jsonArr: Array[JSONObject] = ShareData.jsonArr_ExistOrCreat(jsonFile)
    val splitNum = jsonArr.length
    val partitionCoefficient: Int = this.partitionCoefficient(splitNum)

    val idNameArr_Stable: Array[(String, Double)] = timeSteps.filter(timeStep => timeStep == 12.0d).map(timeStep => jsonArr.map(json0 => {
      val idName = Analysis.analysisJsonIdName(json0)
      (idName, timeStep)
    })).reduce((x, y) => (x.union(y)))
    val idNameMap0: mutable.HashMap[String, Array[(String, Double)]] = ArrayOperation.randomGroup(idNameArr_Stable, partitionCoefficient)

    timeSteps.filter(timeStep => timeStep == 1.0d).foreach(timeStep => {
      jsonArr.foreach(json0 => {
        val idName = Analysis.analysisJsonIdName(json0)
        val groupKey = idName + "_" + timeStep
        idNameMap0.put(groupKey, Array((idName, timeStep)))
      })
    })

    //显示信息无用可注释
    idNameMap0.foreach(f => {
      val groupKey = f._1
      val idNameTimeStep = f._2
      idNameTimeStep.foreach(g => {
        val idName = g._1
        val timeStep = g._2
        val groupKeyStr = this.groupKeyShowStr(idNameMap0, idName, timeStep)
        val msg = "groupKey=" + groupKey + ";" + groupKeyStr
        println(msg)
      })
    })
    println("idNameMaps.size=" + idNameMap0.size)


    idNameMap0
  }

  def groupKeyShowStr(idNameMaps: mutable.HashMap[String, Array[(String, Double)]], idName: String, timeStep: Double): String = {
    val groupKey = this.groupKey(idNameMaps, idName, timeStep)
    "groupKey=" + groupKey + ";idName=" + idName + ";timeStep=" + timeStep + ";host=" + workersUtil.getHostName() + "(" + workersUtil.getIp() + ");ExecutorId=" + workersUtil.getExecutorId()
  }

  def groupKey(idNameMap: mutable.HashMap[String, Array[(String, Double)]], idName: String, timeStep: Double): String = {
    val key = this.getidNameKey1(idNameMap, idName, timeStep)
    if ("".equals(key)) {
      val msg = "groupKey,timeStep=" + timeStep + ",idName=" + idName + "未配置"
      println(msg)
    }
    key
  }

  def getidNameKey1(idNameMap: mutable.HashMap[String, Array[(String, Double)]], idName: String, timeStep: Double): String = {
    var key: String = ""
    idNameMap.foreach(f => {
      val key0: String = f._1
      val value0: Array[(String, Double)] = f._2
      if (value0.contains((idName, timeStep))) {
        key = key0
      }
    })
    key
  }
}
