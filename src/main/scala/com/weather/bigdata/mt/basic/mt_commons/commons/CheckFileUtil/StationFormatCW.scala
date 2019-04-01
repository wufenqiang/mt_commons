package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil

import java.io.FileWriter
import java.util.Date

import com.alibaba.fastjson.JSONObject
import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.WeatherShowtime
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation._
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import org.apache.spark.rdd.RDD

import scala.collection.mutable
/**
  *Created by wufenqiang 2018-7-5
  */
object StationFormatCW {
  /*private def sortSetKey(set:scala.collection.Set[(String,Double)]): (mutable.HashSet[String],mutable.HashSet[Double]) ={
    val fcTypeSet:mutable.HashSet[String]=new mutable.HashSet[String]
    val timeStepSet:mutable.HashSet[Double]=new mutable.HashSet[Double]
    set.foreach(f=>{
      val fcType=f._1
      val timeStep=f._2
      fcTypeSet.+(fcType)
      timeStepSet.+(timeStep)
    })
    if((fcTypeSet.size * timeStepSet.size) != set.size){
      val msg="keySet的(fcType,timeStep)分布不均,请检查数据"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }
    (fcTypeSet,timeStepSet)
  }*/
  /*  private def getCheckMap(StnIdGetLatLon: mutable.HashMap[String,String],scidata:SciDataset): mutable.HashMap[(String,Double),mutable.HashMap[String,mutable.HashMap[Date,Double]]] ={
      val outMap0:mutable.HashMap[(String,Double),mutable.HashMap[String,mutable.HashMap[Date,Double]]]=new mutable.HashMap[(String,Double),mutable.HashMap[String,mutable.HashMap[Date,Double]]]

      val fcType=AttributesOperation.getFcType(scidata)
      val timeStep=AttributesOperation.gettimeStep(scidata)
      val fcdate=AttributesOperation.getfcdate(scidata)
      val (timeName,heightName,latName,lonName)=AttributesOperation.getVarName(scidata)
      val dataNames=scidata.variables.keySet.-(timeName,heightName,latName,lonName).toArray

      val time :Array[Double]= scidata.apply(timeName).data
      val height:Array[Double]=scidata.apply(heightName).data
      val lat :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(latName).data)
      val lon :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(lonName).data)
      val datas:Array[Array[Double]]=dataNames.map(dataName=> scidata.apply(dataName).data)

      val timeLen=time.length
      val heightLen=height.length
      val latLen=lat.length
      val lonLen=lon.length

      if(heightLen!=1){
        val msg="站点数据存在高度,抽取高度序号最低的点。"
        val e:Exception=new Exception(msg)
        e.printStackTrace()
      }

      val beginLat = lat.min
      val endLat = lat.max
      val beginLon = lon.min
      val endLon = lon.max


      for(j <- 0 to dataNames.length-1){
        val outMap1:mutable.HashMap[String,mutable.HashMap[Date,Double]]=new mutable.HashMap[String,mutable.HashMap[Date,Double]]
        StnIdGetLatLon.foreach(kv=>{
          val StnId=kv._1
          val StnLatLon=kv._2
          val (stnlat,stnlon)=ReadFile.analysisLatLon(StnLatLon)
          if (beginLat<= stnlat && stnlat <= endLat && beginLon<=stnlon && stnlon <= endLon) {
            val latIndex :Int= AO.nearIndex_sequence(lat,stnlat)
            val lonIndex :Int= AO.nearIndex_sequence(lon,stnlon)

            val outMap2:mutable.HashMap[Date,Double]=new mutable.HashMap[Date,Double]
            for(i<- 0 to time.length-1){
              val thedate=DateOp.plusHour(fcdate,(timeStep*i).toInt)
              val index=VariableOperation.getIndex4(i,0,latIndex,lonIndex,heightLen,latLen,lonLen)
              val eleValue :Double=datas(j)(index)
              outMap2.put(thedate,eleValue)
            }
            outMap1.put(StnId,outMap2)
          }
        })
        outMap0.put((dataNames(j),timeStep),outMap1)
      }

      outMap0
    }*/
  /*private def writeCheckMap(checkMap:mutable.HashMap[(String,Double),mutable.HashMap[String,mutable.HashMap[Date,Double]]],checkOutPath:String,date:Date): Unit ={
    val (dataNames,timeSteps)=this.sortSetKey(checkMap.keySet)
    val fileHead=AO.addStr(Array("LST"),dataNames.toArray)
    val fileHeadStr=ShowUtil.ArrayStr2Str(fileHead,this.SplitSpace)
    timeSteps.foreach(timeStep=>{
      val fileName=PropertiesUtil.getcheckfileAll(checkOutPath,timeStep,date)
      val tmpPath=PropertiesUtil.getWriteTmp
      val tmpfileName=tmpPath+fileName

      val checkOutfile=checkOutPath+fileName
      if(FileOperation.LocalExist(tmpfileName)){
        FileOperation.LocalDelete(tmpfileName)
      }

      rw.writeHDFSfile(tmpfileName,fileHeadStr)



    })
  }*/

  //  private val AO:ArrayOperation=new ArrayOperation
  private val DateOp: DateOperation = new DateOperation
  private val SetOp=SetOperation
  private val JsonOp=new JsonOperation
  private val ws:WeatherShowtime=new WeatherShowtime
  val timeStepKey="timeStep"
  val DateKey="Date"
  val StnIdKey="StnId"
  val SplitSpace = "          "

  private def sortSet(Keys:mutable.HashSet[(String,Double,Date)]): (mutable.HashSet[String],mutable.HashSet[Double],mutable.TreeSet[Date]) ={
    val key0:mutable.HashSet[String]=new mutable.HashSet[String]()
    val key1:mutable.HashSet[Double]=new mutable.HashSet[Double]()
    val key2:mutable.TreeSet[Date]=new mutable.TreeSet[Date]()
    Keys.foreach(ks=>{
      val k0=ks._1
      val k1=ks._2
      val k2=ks._3
      key0.add(k0)
      key1.add(k1)
      key2.add(k2)
    })
    if((key0.size*key1.size*key2.size)!=Keys.size){
      val msg="sortSet,Key Set has some mistakes."
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }
    (key0,key1,key2)
  }

  private def filewriteOp(out:FileWriter,arrStr:Array[String],splitSpace:String): Unit ={
    for(i<- 0 to arrStr.length-1){
      out.write(arrStr(i))
      out.write(splitSpace)
    }
    out.write("\n")
  }
  private def filewriteOp(out:FileWriter,str:String,splitSpace:String): Unit ={
    this.filewriteOp(out,Array(str),splitSpace)
  }

  private def JsonStrKey(timeStep:Double,stnId:String,thedate:Date):String={
    val jsonKey:JSONObject=new JSONObject
    val thedateStr = DateFormatUtil.YYYYMMDDHHMMSSStr1(thedate)
    jsonKey.put(this.timeStepKey,timeStep)
    jsonKey.put(this.DateKey,thedateStr)
    jsonKey.put(this.StnIdKey,stnId)
    jsonKey.toJSONString
  }
  private def analysisJsonStrKey(jsonStr:String): (String,Double,Date) ={
    val jsonKey:JSONObject=JsonOp.Str2JSONObject(jsonStr)
    val timeStep=jsonKey.getDouble(this.timeStepKey)
    val stnId=jsonKey.getString(this.StnIdKey)
    val thedateStr:String=jsonKey.getString(this.DateKey)
    val thedate: Date = DateFormatUtil.YYYYMMDDHHMMSS1(thedateStr)
    (stnId,timeStep,thedate)
  }
  /*def getCheckJson(StnIdGetLatLon: mutable.HashMap[String,String],scidata:SciDataset): JSONObject ={
    val fcType=AttributesOperation.getFcType(scidata)
    val timeStep=AttributesOperation.gettimeStep(scidata)
    val fcdate=AttributesOperation.getfcdate(scidata)
    val (timeName,heightName,latName,lonName)=AttributesOperation.getVarName(scidata)
    val dataNames=scidata.variables.keySet.-(timeName,heightName,latName,lonName).toArray

    val time :Array[Double]= scidata.apply(timeName).data
    val height:Array[Double]=scidata.apply(heightName).data
    val lat :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(latName).data)
    val lon :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(lonName).data)
    val datas:Array[Array[Double]]=dataNames.map(dataName=> scidata.apply(dataName).data)

    val timeLen=time.length
    val heightLen=height.length
    val latLen=lat.length
    val lonLen=lon.length

    if(heightLen!=1){
      val msg="站点数据存在高度,抽取高度序号最低的点。"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }

    val beginLat = lat.min
    val endLat = lat.max
    val beginLon = lon.min
    val endLon = lon.max

    val jsonArr:Array[JSONObject]=StnIdGetLatLon.map(
      kv=>{
        val StnId=kv._1
        val StnLatLon=kv._2
        val (stnlat,stnlon)=ReadFile.analysisLatLon(StnLatLon)
        val jsonArr1:Array[JSONObject]=time.map(t=>{
          val outjson:JSONObject=new JSONObject()
          if(beginLat<= stnlat && stnlat <= endLat && beginLon<=stnlon && stnlon <= endLon){
            val latIndex :Int= AO.nearIndex_sequence(lat,stnlat)
            val lonIndex :Int= AO.nearIndex_sequence(lon,stnlon)
            val theDate=DateOp.plusHour(fcdate,t.toInt)
            val theDateStr=DataFormatUtil.YYYYMMDDHHStr1(theDate)
            for(i<- 0 to dataNames.length-1){
              val dataName=dataNames(i)
              val index=VariableOperation.getIndex4_thlatlon(i,0,latIndex,lonIndex,heightLen,latLen,lonLen)
              val eleValue:Double=datas(i)(index)
              outjson.put(dataName,eleValue)
            }
            outjson.put(this.timeStepKey,timeStep)
            outjson.put(this.DateKey,theDateStr)
            outjson.put(this.StnIdKey,StnId)
          }
          outjson
        }).filter(p=>(p.size!=0))
        jsonArr1
      }
    ).filter(p=>(p.size!=0)).reduce((x,y)=>(x.union(y)))
    val outJson:JSONObject=new JSONObject()
    jsonArr.foreach(jso=>{
      val timeStep:Double=jso.get(this.timeStepKey).toString.toDouble
      val dateStr:String=jso.get(this.DateKey).toString
      val thedate=DataFormatUtil.YYYYMMDDHH1(dateStr)
      val stnId:String=jso.get(this.StnIdKey).toString
      val k=this.checkJsonKey(timeStep,stnId,thedate)
      outJson.put(k,jso)
    })
    outJson
  }*/
  /*def getCheckMap(StnIdGetLatLon: mutable.HashMap[String,String],scidata:SciDataset): mutable.HashMap[(String,Double,Date),JSONObject] ={
    val map:mutable.HashMap[(String,Double,Date),JSONObject]=new mutable.HashMap[(String,Double,Date),JSONObject]()


    val fcType=AttributesOperation.getFcType(scidata)
    val timeStep0=AttributesOperation.gettimeStep(scidata)
    val fcdate=AttributesOperation.getfcdate(scidata)
    val (timeName,heightName,latName,lonName)=AttributesOperation.getVarName(scidata)
    val dataNames=scidata.variables.keySet.-(timeName,heightName,latName,lonName).toArray

    val time :Array[Double]= scidata.apply(timeName).data
    val height:Array[Double]=scidata.apply(heightName).data
    val lat :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(latName).data)
    val lon :Array[Double]= DataFormatUtil.doubleFormat2(scidata.apply(lonName).data)
    val datas:Array[Array[Double]]=dataNames.map(dataName=> scidata.apply(dataName).data)

    val timeLen=time.length
    val heightLen=height.length
    val latLen=lat.length
    val lonLen=lon.length

    if(heightLen!=1){
      val msg="站点数据存在高度,抽取高度序号最低的点。"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }

    val beginLat = lat.min
    val endLat = lat.max
    val beginLon = lon.min
    val endLon = lon.max
    //    val outJson0:JSONObject=new JSONObject()
    //    val timeSteps=Array(timeStep0)

    /*val outJsonArr:Array[JSONObject]=StnIdGetLatLon.map(
      kv=>{
        val StnId=kv._1
        val StnLatLon=kv._2
        val (stnlat,stnlon)=ReadFile.analysisLatLon(StnLatLon)
        val outJson1:JSONObject=new JSONObject()
        if(beginLat<= stnlat && stnlat <= endLat && beginLon<=stnlon && stnlon <= endLon){
          timeSteps.foreach(timeStep=>{
            val outJson2:JSONObject=new JSONObject()
            for(j<- 0 to timeLen-1){
              val outJson3:JSONObject=new JSONObject()
              val latIndex :Int= AO.nearIndex_sequence(lat,stnlat)
              val lonIndex :Int= AO.nearIndex_sequence(lon,stnlon)
              val t:Double=time(j)
              val theDate=DateOp.plusHour(fcdate,t.toInt)
              val theDateStr=DataFormatUtil.YYYYMMDDHHStr1(theDate)
              for(i<- 0 to dataNames.length-1){
                val dataName=dataNames(i)
                val index=VariableOperation.getIndex4_thlatlon(i,0,latIndex,lonIndex,heightLen,latLen,lonLen)
                val eleValue:Double=datas(i)(index)
                outJson3.put(dataName,eleValue)
              }
              outJson3.put(this.timeStepKey,timeStep)
              outJson3.put(this.DateKey,theDateStr)
              outJson3.put(this.StnIdKey,StnId)
              outJson2.put(theDateStr,outJson3)
            }
            outJson1.put(timeStep.toString,outJson2)
          })
        }
        outJson1
      }
    ).filter(p=>(!(p.isEmpty))).toArray
    outJsonArr*/
    StnIdGetLatLon.foreach(
      kv=>{
        val StnId=kv._1
        val StnLatLon=kv._2
        val (stnlat,stnlon)=ReadFile.analysisLatLon(StnLatLon)
        val outJson1:JSONObject=new JSONObject()
        if(beginLat<= stnlat && stnlat <= endLat && beginLon<=stnlon && stnlon <= endLon){

          //            val outJson2:JSONObject=new JSONObject()
          for(j<- 0 to timeLen-1){
            val outJson3:JSONObject=new JSONObject()
            val latIndex :Int= AO.nearIndex_sequence(lat,stnlat)
            val lonIndex :Int= AO.nearIndex_sequence(lon,stnlon)
            val t:Double=time(j)
            val theDate=DateOp.plusHour(fcdate,t.toInt)
            val theDateStr=DataFormatUtil.YYYYMMDDHHStr1(theDate)
            for(i<- 0 to dataNames.length-1){
              val dataName=dataNames(i)
              val index=VariableOperation.getIndex4_thlatlon(i,0,latIndex,lonIndex,heightLen,latLen,lonLen)
              val eleValue:Double=datas(i)(index)
              outJson3.put(dataName,eleValue)
            }
            outJson3.put(this.timeStepKey,timeStep0)
            outJson3.put(this.DateKey,theDate)
            outJson3.put(this.StnIdKey,StnId)
            //              outJson2.put(theDateStr,outJson3)
            map.put((StnId,timeStep0,theDate),outJson3)
          }
          //            outJson1.put(timeStep.toString,outJson2)

          //          outJson0.put(StnId,outJson1)
        }
      }
    )
    //    outJson0

    map
  }*/

  private def getEleSet(stnIdsSet:mutable.HashSet[String],timeStepsSet:mutable.HashSet[Double],datesSet:mutable.TreeSet[Date],checkJson:JSONObject): mutable.HashSet[String] ={
    val stnid:String=SetOp.RamdomString(stnIdsSet)
    val timeStep:Double=SetOp.RamdomDouble(timeStepsSet)
    val theDate:Date=SetOp.RamdomDate(datesSet)
    val keyStr=this.JsonStrKey(timeStep,stnid,theDate)
    val valueJson:JSONObject=checkJson.getJSONObject(keyStr)
    val eleSet:mutable.HashSet[String]=SetOp.Set2scalaSet(valueJson.keySet())
    eleSet
  }
  private def getKeySet(checkJson:JSONObject): (mutable.HashSet[String],mutable.HashSet[Double],mutable.TreeSet[Date]) ={
    val checkJsonKeys: mutable.HashSet[String]=SetOp.Set2scalaSet(checkJson.keySet())
    val KeySet:mutable.HashSet[(String,Double,Date)]=checkJsonKeys.map(k=>this.analysisJsonStrKey(k))
    val (stnIdsSet:mutable.HashSet[String],timeStepsSet:mutable.HashSet[Double],datesSet:mutable.TreeSet[Date])=this.sortSet(KeySet)
    (stnIdsSet,timeStepsSet,datesSet)
  }
  private def getAllSets(checkJson:JSONObject): (mutable.HashSet[String],mutable.HashSet[Double],mutable.TreeSet[Date],mutable.HashSet[String]) ={
    val (stnIdsSet:mutable.HashSet[String],timeStepsSet:mutable.HashSet[Double],datesSet:mutable.TreeSet[Date])=this.getKeySet(checkJson)
    val eleSet:mutable.HashSet[String]=this.getEleSet(stnIdsSet,timeStepsSet,datesSet,checkJson)
    (stnIdsSet,timeStepsSet,datesSet,eleSet)
  }

  private def reduceByStation(json0:JSONObject,json1:JSONObject):JSONObject ={
    if(json0.size()==0 && json1.size()==0){
      json0
    }else if(json0.size()==0 && json1.size()!=0){
      json1
    }else if(json0.size()!=0 && json1.size()==0){
      json0
    }else{
      if(json0.size()<json1.size()){
        val checkJsonKeys: mutable.HashSet[String]=SetOp.Set2scalaSet(json0.keySet())
        val keys:Array[String]=checkJsonKeys.toArray
        for(i<- 0 to keys.length-1){
          val k=keys(i)
          val v:JSONObject=json0.getJSONObject(k)
          json1.put(k,v)
        }
        json1
      }else{
        val checkJsonKeys: mutable.HashSet[String]=SetOp.Set2scalaSet(json1.keySet())
        val keys:Array[String]=checkJsonKeys.toArray
        for(i<- 0 to keys.length-1){
          val k=keys(i)
          val v:JSONObject=json1.getJSONObject(k)
          json0.put(k,v)
        }
        json0
      }
    }
  }
  def getCheckJSON(StnIdGetLatLon: mutable.HashMap[String,String], scidata:SciDatasetCW): JSONObject ={
    val outJson0:JSONObject=new JSONObject

    val fcType=AttributesOperationCW.getFcType(scidata)
    val timeStep=AttributesOperationCW.gettimeStep(scidata)
    val fcdate=AttributesOperationCW.getfcdate(scidata)
    val (timeName,heightName,latName,lonName)=AttributesOperationCW.getVarName(scidata)
    val dataNames=scidata.variables.keySet.-(timeName,heightName,latName,lonName).toArray

    val time :Array[Double]= scidata.apply(timeName).dataDouble()
    val height:Array[Double]=scidata.apply(heightName).dataDouble
    val lat: Array[Double] = NumOperation.K2dec(scidata.apply(latName).dataDouble)
    val lon: Array[Double] = NumOperation.K2dec(scidata.apply(lonName).dataDouble)
    val datas:Array[Array[Double]]=dataNames.map(dataName=> scidata.apply(dataName).dataDouble)

    val timeLen=time.length
    val heightLen=height.length
    val latLen=lat.length
    val lonLen=lon.length

    if(heightLen!=1){
      val msg="站点数据存在高度,抽取高度序号最低的点。"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }

    val beginLat = lat.min
    val endLat = lat.max
    val beginLon = lon.min
    val endLon = lon.max

    StnIdGetLatLon.foreach(
      kv=>{
        val StnId=kv._1
        val StnLatLon=kv._2
        val (stnlat,stnlon)=ReadFile.analysisLatLon(StnLatLon)
        if(beginLat<= stnlat && stnlat <= endLat && beginLon<=stnlon && stnlon <= endLon){
          for(j<- 0 to timeLen-1){
            val valueJson:JSONObject=new JSONObject()
            val latIndex: Int = ArrayOperation.nearIndex_sequence(lat, stnlat)
            val lonIndex: Int = ArrayOperation.nearIndex_sequence(lon, stnlon)
            val t:Double=time(j)
            val theDate=DateOp.plusHour(fcdate,t.toInt)
            val theDateStr = DateFormatUtil.YYYYMMDDHHStr1(theDate)
            val index = IndexOperation.getIndex4_thlatlon(j, 0, latIndex, lonIndex, heightLen, latLen, lonLen)

            for(i<- 0 to dataNames.length-1){
              val dataName=dataNames(i)
              val eleValue:Double=datas(i)(index)
              valueJson.put(dataName,eleValue)
            }
            val keyStr=this.JsonStrKey(timeStep,StnId,theDate)

            outJson0.put(keyStr,valueJson)
          }
        }
      }
    )
    outJson0
  }
  def getCheckJSON(StnIdGetLatLon: mutable.HashMap[String,String], rdd:RDD[SciDatasetCW]): RDD[JSONObject] ={
    rdd.map(scidata=>this.getCheckJSON(StnIdGetLatLon,scidata))
  }

  def writeCheckMap(checkJson:JSONObject,timeStep:Double,checkOutPath:String,fileName:String): Unit ={
    val (stnIdsSet:mutable.HashSet[String],timeStepsSet:mutable.HashSet[Double],datesSet:mutable.TreeSet[Date],dataNamesSet:mutable.HashSet[String])=this.getAllSets(checkJson)
    if(timeStepsSet.contains(timeStep)){
      val dataNames:List[String]=dataNamesSet.toList
      val fileHead: Array[String] = ArrayOperation.addStr(Array("LST"), dataNames.toArray)

      val tmpPath=PropertiesUtil.getWriteTmp
      val tmpfileName=tmpPath+fileName
      if(HDFSOperation1.exists(tmpfileName)){
        HDFSOperation1.delete(tmpfileName)
      }

      HDFSOperation1.createFile(tmpfileName)
      println("创建tmpfileName="+tmpfileName)

      val out = new FileWriter(tmpfileName,true)
      this.filewriteOp(out,fileHead,SplitSpace)
      val flag:Boolean=stnIdsSet.map(
        station_id=>{
          this.filewriteOp(out,Array(station_id),SplitSpace)
          val flag0:Boolean=datesSet.map(
            thedate=>{
              val line:Array[String]=new Array[String](dataNames.length+1)
              line(0) = DateFormatUtil.YYYYMMDDHHStr1(thedate)
              val KeyStr=this.JsonStrKey(timeStep,station_id,thedate)
              val ValuesJson:JSONObject=checkJson.getJSONObject(KeyStr)
              for(i<- 0 to dataNames.length-1){
                val dataName=dataNames(i)
                line(i+1)=ValuesJson.getString(dataName)
              }
              try{
                this.filewriteOp(out,line,SplitSpace)
                true
              }catch{
                case e:Exception=>{
                  e.printStackTrace()
                  false
                }
              }
            }
          ).reduce((x,y)=>(x && y ))
          flag0
        }
      ).reduce((x,y)=>(x && y))
      if(flag){
        out.close()
        HDFSOperation1.copyfile(tmpfileName,checkOutPath,true,true)
      }
    }else{
      val msg="不在checkJson信息中不包含timeStep="+timeStep
      val e:Exception=new Exception(msg)
      e.printStackTrace()
    }
  }

  /*def main(args:Array[String]): Unit ={
    val fileName="/D:/Data/forecast/Output/NMC/20180308/08/WIND/1h/"
    println(fileName)
//    val rdd:RDD[SciDataset]=ReadFile.ReadNcRdd(fileName)
    val StnIdGetLatLon=ReadFile.ReadStationInfoMap_IdGetlatlon1()
    val checkJsons:RDD[JSONObject]=this.getCheckJSON(StnIdGetLatLon,rdd)

    val checkJson=checkJsons.reduce((x,y)=>this.reduceByStation(x,y))

    val checkOutPath="/D:/Data/forecast/Output/"
    val OutfileName="StationFormatTest.txt"
    val timeStep=1.0d

    this.writeCheckMap(checkJson,timeStep,checkOutPath,OutfileName)
  }*/
}
