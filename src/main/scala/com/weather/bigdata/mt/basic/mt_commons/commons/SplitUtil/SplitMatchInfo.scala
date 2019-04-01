package com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil

object SplitMatchInfo {
  def oldName2newName(sciDatasetName_old:String,idName:String,timeStep:Double): String ={
    val datanameold=sciDatasetName_old.replace(".nc","")
    idName+"_"+datanameold+"_"+timeStep+".nc"
  }
  def oldName2newName(sciDatasetName_old:String,idName:String): String ={
    val datanameold=sciDatasetName_old.replace(".nc","")
    idName+"_"+datanameold+".nc"
  }
  private def oldName2newName(sciDatasetName_old:String): String ={
    val datanameold=sciDatasetName_old.replace(".nc","")
    datanameold+".nc"
  }


  def newName2oldName(sciDatasetName_new:String): String =sciDatasetName_new.split("_")(1).replace(".nc","")+".nc"


  def oldVar2newVar(dataName:String,alType_timeIA:String): String ={
    dataName+"_"+alType_timeIA
  }
  def newVar2oldVar(dataName_alType_timeIA:String): String ={
    dataName_alType_timeIA.split("_")(0)
  }

  def reduceAppId(fileName:String,appId:String): String ={
    fileName.replace(appId,"")
  }
  def main(args:Array[String]): Unit ={
    val idName:String="0"
    val FcType="PRE"
    val timeStep=12.0d
    println(SplitMatchInfo.oldName2newName(FcType,idName,timeStep))
  }
}
