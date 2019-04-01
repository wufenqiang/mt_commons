package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import java.util
import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import com.weather.bigdata.it.utils.{ShowUtil, WeatherShowtime}
import com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil.CheckStationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.InterpolationUtil.TimeInterpolationCW
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.WriteNcCW
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.{MatchSciDatasCW, SplitMatchInfo}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.workersUtil
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object RddKVStoreCW{
  //v0：12h入库切分组数：份数根据1h和12h数据的比例调优,1h为72个时次,12h为10个时次,以及总块数77，分组数≈ 总块数77/(1h时次/12h时次)=10
  //v1：12h入库切分组数：份数根据1h和12h数据的比例调优,1h入库时间800-900s,12h入库时间200s,以及总块数77，分组数≈ 总块数/(1h时次/12h时次)=20
  //v2：12h最大不超过3
  //设置与sparkJobSubmit.partitionCoefficient相同
  //  val partitionCoefficient:Int=sparkJobSubmit.partitionCoefficient
  //  val partitionCoefficient:Int=26
  //延时等待时间ms


  //入库前数据输出开关/输出地址
  private val inputKVStoreOutOpen:Boolean =PropertiesUtil.inputKVStoreOutOpen

  //入库数据输出抽查站开关
  private val checkStationOpen:Boolean =PropertiesUtil.checkStationOpen


  private def sci2Array3(scidata:SciDatasetCW, timeName:String, heightName:String, latName:String, lonName:String, dataName:String):(Array[Array[Array[Float]]],Array[Int])={
    if(scidata!=null){

      val timeV=scidata.apply(timeName)
      val heightV=scidata.apply(heightName)
      val latV=scidata.apply(latName)
      val lonV=scidata.apply(lonName)
      val dataV0=scidata.apply(dataName)

      val time:Array[Float]=timeV.dataFloat
      val height:Array[Float]=heightV.dataFloat
      val lat:Array[Float]=latV.dataFloat
      val lon:Array[Float]=lonV.dataFloat
      //      val data0:Array[Float]=scidata.apply(dataName).dataFloat


      val timeLen=timeV.getLen()
      val heightLen=heightV.getLen()
      val latLen=latV.getLen()
      val lonLen=lonV.getLen()


      if(heightLen != 1){
        println("height.length="+heightLen+",默认取值height(0)位置")
      }

      val timeIndexs=Array.range(0,timeLen,1)
      val latIndexs=Array.range(0,latLen,1)
      val lonIndexs=Array.range(0,lonLen,1)
      val heightIndex:Int=0

      val arr:Array[Array[Array[Float]]]={
        timeIndexs.map(timeIndex=>{
          latIndexs.map(latIndex=>{
            lonIndexs.map(lonIndex=>{
              val index: Int = IndexOperation.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, heightLen, latLen, lonLen)
              //              data0(index)
              dataV0.dataFloat(index)
            })
          })
        })
      }

      val lenArr=Array(timeLen,latLen,lonLen)
      println("sci2Array3:scidata.dataName="+scidata.datasetName+",timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen)
      (arr,lenArr)
    }else{
      null
    }
  }
  /*private def sci2Arr3Maps(scidata:SciDataset,timeName:String,heightName:String,latName:String,lonName:String,dataNames:Array[String]):util.HashMap[String, AnyRef]={
    if(scidata!=null){

      val time:Array[Double]=scidata.apply(timeName).data
      val height:Array[Double]=scidata.apply(heightName).data
      val lat:Array[Double]=scidata.apply(latName).data
      val lon:Array[Double]=scidata.apply(lonName).data



      val timeLen=time.length
      val heightLen=height.length
      val latLen=lat.length
      val lonLen=lon.length


      if(heightLen != 1){
        println("height.length="+heightLen+",默认取值height(0)位置")
      }

      val timeIndexs=Array.range(0,timeLen,1)
      val latIndexs=Array.range(0,latLen,1)
      val lonIndexs=Array.range(0,lonLen,1)
      val heightIndex:Int=0

      val map:util.HashMap[String, AnyRef]=new util.HashMap[String, AnyRef]
      dataNames.foreach(dataName=>{
        val data0:Array[Double]=scidata.apply(dataName).data
        val arr:Array[Array[Array[Double]]]={
          timeIndexs.map(timeIndex=>{
            latIndexs.map(latIndex=>{
              lonIndexs.map(lonIndex=>{
                val index:Int=VariableOperation.getIndex4_thlatlon(timeIndex,heightIndex,latIndex,lonIndex,heightLen,latLen,lonLen)
                data0(index)
              })
            })
          })
        }
        println("sci2Array3:scidata.dataName="+scidata.datasetName+",timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen)
        map.put(dataName,arr)
      })
      map
    }else{
      null
    }
  }*/

  /*def rdd2Array3_2(date:Date, arrRdd:Array[RDD[SciDataset]], timeName:String, heightName:String, latName:String, lonName:String): Unit ={
    val Rdds: RDD[(Int,Iterable[SciDataset])]=MatchSciDatas.matchByName1(arrRdd)
    println("rdd2Array3_1,arrRDD.length="+arrRdd.length)
    val dateStr=DataFormatUtil.YYYYMMDDHHMMStr(date)


    val reRdds:RDD[(Int,(Int,Int),util.HashMap[String, AnyRef],Array[Int],String)]=Rdds.map(
       scidatas=>{
         val idName=scidatas._1
         val arrScidatas=scidatas._2

         val GS:SciDataset=arrScidatas.last
         val timeStep:Double=AttributesOperation.gettimeStep(GS)

         val (timeV,heightV,latV,lonV)=VO.getVars(GS,timeName,heightName,latName,lonName)

         val time=timeV.data
         val height=heightV.data
         val latSplit=DataFormatUtil.doubleFormat(latV.data)
         val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

         val (lat,lon)=PropertiesUtil.getLatLon()

         val latIndex=AO.HashMapSearch(lat,latSplit(0))
         val lonIndex=AO.HashMapSearch(lon,lonSplit(0))

         val timeLen=time.length
         val heightLen=height.length
         val latLen=latSplit.length
         val lonLen=latSplit.length


         val dataArr:Iterable[util.HashMap[String, AnyRef]]=arrScidatas.map(
           scidata=>{
             val fcType=AttributesOperation.getFcType(scidata)
             val timeStep=AttributesOperation.gettimeStep(scidata)

             val dataNames:Array[String]={
               if(timeStep==1){
                 PropertiesUtil.getwElement(fcType,timeStep)
               }else if(timeStep==12){
                 val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
                 WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
               }else{
                 val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
                 null
               }
             }

             val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
               dataName=>{
                 val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]
                 val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
                 val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

                 data00.put(key,arr)
                 data00
               }
             )
             val data0:util.HashMap[String, AnyRef]=dataarr0.reduce(
               (x,y)=>{
                 x.putAll(y)
                 x
               }
             )
             data0
           }
         )
         val data:util.HashMap[String, AnyRef]=dataArr.reduce(
           (x,y)=>{
             x.putAll(y)
             x
           }
         )

         (idName,(latIndex,lonIndex),data,Array[Int](timeLen, latLen,lonLen),dateStr)
         /*val types:String =  PropertiesUtil.getKVStoreType(timeStep)
         val helper = new KVStoreSimpleHelper(this.metaurl,types,this.timeout)
         helper.open()
         try{
           helper.write(latIndex, lonIndex, data, Array[Int](timeLen, latLen,lonLen), dateStr)
           val msg="data.size="+data.size()+";idName="+idName+";dateStr="+dateStr+";latIndex="+latIndex+";lonIndex="+lonIndex+";rdd2Array3_1入库成功"
           println(msg)
         }catch{
           case e: Exception => {
             val e0:Exception=new Exception("data.size="+data.size()+"idName="+idName+";dateStr="+dateStr+";latIndex="+latIndex+";lonIndex="+lonIndex+";rdd2Array3_1入库失败!!!"+e)
             e0.printStackTrace()
             throw(e0)
           }
         }finally {
           helper.close()
         }*/
       }
     )

  }*/
  private def rddInputKV(obsdate:Date,idNameTimeStepArrScidatas:(Any,Iterable[SciDatasetCW]),stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String): Boolean ={
    val groupKey=idNameTimeStepArrScidatas._1
    val arrScidatas=idNameTimeStepArrScidatas._2

    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val GS:SciDatasetCW=arrScidatas.last
    val GSfcType=AttributesOperationCW.getFcType(GS)
    val idName=AttributesOperationCW.getIDName(GS)
    val timeStep=AttributesOperationCW.gettimeStep(GS)

    val msg="GSfcType="+GSfcType+";idName="+idName+";timeStep="+timeStep
    println(msg)


    val (timeName, heightName, latName, lonName)=AttributesOperationCW.getVarName(GS)

    //    val (timeV,heightV,latV,lonV)=SciDatasetOperationCW.getVars(GS,timeName,heightName,latName,lonName)
    val timeV=GS.apply(timeName)
    val heightV=GS.apply(heightName)
    val latV=GS.apply(latName)
    val lonV=GS.apply(lonName)


    //    val time=timeV.dataFloat
    //    val height=heightV.dataFloat
    val latSplit:Array[Float]= NumOperation.Kdec(latV.dataFloat)
    val lonSplit:Array[Float] = NumOperation.Kdec(lonV.dataFloat)

    val (lat, lon) = PropertiesUtil.getLatLon(PropertiesUtil.latlonStep)

    val latIndex=ArrayOperation.HashMapSearch(lat,latSplit.head)
    val lonIndex=ArrayOperation.HashMapSearch(lon,lonSplit.head)

    val timeLen=timeV.getLen
    val heightLen=heightV.getLen
    val latLen=latV.getLen
    val lonLen=lonV.getLen


    val types:String =  PropertiesUtil.getKVStoreType(timeStep,idName)


    val (retCode:Long,retStr:String)={
      val realHost:String={
        try{
          val host_ports: Array[(String, String, Int, String)] = KVStoreConf.gethost_port(PropertiesUtil.getKVStoreURL(), types)
          host_ports.map(f=>(f._1+"="+f._2+":"+f._3)).reduce((x,y)=>(x+";"+y))
        }catch {
          case e:Exception=>{
            e.printStackTrace()
            ""
          }
        }
      }

      val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
      val bakMsg = "设置入库地址=" + PropertiesUtil.getKVStoreURL() + ";设置等待时间=" + PropertiesUtil.getKVtimeOut() + "ms;入库reftime=" + dateStr + ";入库Key=" + types + ";" + groupKey + ";实际库地址:" + realHost
      val olddateStr: String = DateFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr, timeStep))
      val obsdateStr: String = DateFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

      val dataArr:Iterable[util.HashMap[String, AnyRef]]=arrScidatas.map(
        scidata=>{
          val fcType=AttributesOperationCW.getFcType(scidata)

          //输出入库前nc //入库前站点抽查
          if(this.inputKVStoreOutOpen || this.checkStationOpen){
            val timeStep0={
              if(timeStep==12.0d){
                timeStep * 2
              }else{
                timeStep
              }
            }
            this.updateTimeStepPlusName(scidata,timeStep0)

            if(this.inputKVStoreOutOpen){
              val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
              val start_Write=System.currentTimeMillis()
              WriteNcCW.WriteNcFile_addID_tmp(scidata, inputKVStorePath, appID)
              val end_Write=System.currentTimeMillis()
              WeatherShowtime.showDateStrOut1("入库前数据输出",start_Write,end_Write,"输出入库前数据开关(pom.inputKVStoreOut.Open="+this.inputKVStoreOutOpen+")开启;idName="+idName+";fcType="+fcType+";timeStep="+timeStep+"输出;输出地址为:"+inputKVStorePath)
            }else{
              println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
            }

            if(this.checkStationOpen){
              println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
              val stationsInfo=stationsInfos._1
              val stationinputKV=stationsInfos._2
              CheckStationCW.checkStation(appID,stationsInfo,stationinputKV,scidata,obsdate,fcdate)
            }else{
              println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
            }
          }


          val dataNames:Array[String]={
            if(timeStep==1.0d ){
              PropertiesUtil.getwElement(fcType,1.0d)
            }else if(timeStep==12.0d || timeStep==24.0d){
              val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,12.0d)
              TimeInterpolationCW.getnewdataName(dataNames_alTypetimeIAs)
            }else{
              val e:Exception=new Exception("rddInputKV,timeStep="+timeStep+"没有入库操作")
              e.printStackTrace()
              null
            }
          }

          val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
            dataName=>{
              val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, Object]
              val (arr:Array[Array[Array[Float]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
              val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

              data00.put(key,arr)
              data00
            }
          )
          val data0:util.HashMap[String, AnyRef]=dataarr0.reduce(
            (x,y)=>{
              x.putAll(y)
              x
            }
          )
          data0
        }
      )
      val hashdata:util.HashMap[String, AnyRef]=dataArr.reduce(
        (x,y)=>{
          x.putAll(y)
          val nowStr: String = DateFormatUtil.YYYYMMDDHHMMSSStr1(new Date( ))
          println("入库数据准备:"+nowStr+",idName="+idName+",timeStep="+timeStep+",data.size="+x.size()+";"+ShowUtil.MapShowKeys(x))+";"+groupKey
          x
        }
      )
      println(groupKey+";rddInputKV0,hashdata.size="+hashdata.size)


      val helper = KVHelper.KVHelper(PropertiesUtil.getKVStoreURL(), types, PropertiesUtil.getKVtimeOut())

      println("入库连接中:" + DateFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis( )) + "......" + bakMsg)
      helper.open()

      val KVStoreStart=System.currentTimeMillis()
      println("入库开始:" + DateFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreStart) + ";" + bakMsg)
      val retCode0:Long=helper.writeWithException(latIndex, lonIndex, hashdata, Array[Int](timeLen, latLen,lonLen), dateStr)
      val KVStoreEnd=System.currentTimeMillis()
      println("入库结束:" + DateFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreEnd) + ";" + bakMsg)

      helper.close()
      println("入库关闭:" + DateFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis( )) + ";" + bakMsg)


      val localIP: String = workersUtil.getIp( )
      if(retCode0>0){
        WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"groupKey="+groupKey+";timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }else{
        WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"groupKey="+groupKey+";timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }

      (retCode0,"")
    }

    //----------------------------入库日志采集------------------------------
    //    val ipStr = workersUtil.getIp(PropertiesUtil.KVStoreURL)
    //    var basicRemark:JSONObject=new JSONObject
    //    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    //    basicRemark=AttributesOperation.addAsyOpen(basicRemark)
    //    basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
    //    basicRemark=AttributesOperation.addAppId(basicRemark,appID)
    //    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    //    basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
    //    val (usableStr:String,legalStr:String)={
    //      if(retCode <0){
    //        ("N","N")
    //      }else{
    //        ("Y","Y")
    //      }
    //    }
    //    var stateRemark:JSONObject=new JSONObject()
    //    stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
    //    SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
    //----------------------------------------------------------------------------

    if(retCode <0){
      false
    }else{
      true
    }

  }
  /*private def rddInputKV_Map(obsdate:Date,idNameTimeStepArrScidatas:(Any,Iterable[SciDataset]),stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String): Boolean ={
    val groupKey=idNameTimeStepArrScidatas._1
    val arrScidatas=idNameTimeStepArrScidatas._2

    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val GS:SciDataset=arrScidatas.last
    val idName=AttributesOperation.getIDName(GS)
    val timeStep=AttributesOperation.gettimeStep(GS)

    val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

    val timeLen=GS.apply(timeName).data.length
    val heightLen=GS.apply(heightName).data.length

    val latSplit=DataFormatUtil.doubleFormat(GS.apply(latName).data)
    val lonSplit=DataFormatUtil.doubleFormat(GS.apply(lonName).data)

    val latlonStep:Double=0.01d
    val (lat,lon)=PropertiesUtil.getLatLon(latlonStep)

    val latIndex=ArrayOperation.HashMapSearch(lat,latSplit.head)
    val lonIndex=ArrayOperation.HashMapSearch(lon,lonSplit.head)

    val latLen=latSplit.length
    val lonLen=lonSplit.length


    val types:String =  PropertiesUtil.getKVStoreType(timeStep,idName)


    val (retCode:Long,retStr:String)={
      val realHost:String={
        try{
          val host_ports:Array[(String,String,Int,String)]=KVStoreConf.gethost_port(this.metaurl,types)
          host_ports.map(f=>(f._1+"="+f._2+":"+f._3)).reduce((x,y)=>(x+";"+y))
        }catch {
          case e:Exception=>{
            e.printStackTrace()
            ""
          }
        }
      }

      val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
      val bakMsg="设置入库地址="+this.metaurl+";设置等待时间="+this.timeout+"ms;入库reftime="+dateStr+";入库Key="+types+";"+groupKey+";实际库地址:"+realHost
      val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
      val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

      val dataArr:Iterable[util.HashMap[String, AnyRef]]=arrScidatas.map(
        scidata=>{
          val fcType=AttributesOperation.getFcType(scidata)

          //输出入库前nc //入库前站点抽查
          if(this.inputKVStoreOutOpen || this.checkStationOpen){
            val timeStep0={
              if(timeStep==12.0d){
                timeStep * 2
              }else{
                timeStep
              }
            }
            val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

            if(this.inputKVStoreOutOpen){
              val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
              val start_Write=System.currentTimeMillis()
              WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
              val end_Write=System.currentTimeMillis()
              WeatherShowtime.showDateStrOut1("入库前数据输出",start_Write,end_Write,"输出入库前数据开关(pom.inputKVStoreOut.Open="+this.inputKVStoreOutOpen+")开启;idName="+idName+";fcType="+fcType+";timeStep="+timeStep+"输出;输出地址为:"+inputKVStorePath)
            }else{
              println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
            }

            if(this.checkStationOpen){
              println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
              val stationsInfo=stationsInfos._1
              val stationinputKV=stationsInfos._2
              CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,obsdate,fcdate)
            }else{
              println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
            }
          }

          val dataNames:Array[String]={
            if(timeStep==1){
              PropertiesUtil.getwElement(fcType,timeStep)
            }else if(timeStep==12){
              val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
              WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
            }else{
              val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
              null
            }
          }

          val data0:util.HashMap[String, AnyRef]=this.sci2Arr3Maps(scidata,timeName,heightName,latName,lonName,dataNames)

          data0
        }
      )
      val hashdata:util.HashMap[String, AnyRef]=dataArr.reduce(
        (x,y)=>{
          x.putAll(y)
          val nowStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr1(new Date())
          println("入库数据准备:"+nowStr+",idName="+idName+",timeStep="+timeStep+",data.size="+x.size()+";"+ShowUtil.MapShowKeys(x))+";"+groupKey
          x
        }
      )
      println(groupKey+";rddInputKV0,hashdata.size="+hashdata.size)


      val helper =KVHelper.KVHelper(this.metaurl,types,this.timeout)

      println("入库连接中:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis())+"......"+bakMsg)
      helper.open()

      val KVStoreStart=System.currentTimeMillis()
      println("入库开始:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreStart)+";"+bakMsg)
      val retCode0:Long=helper.writeWithException(latIndex, lonIndex, hashdata, Array[Int](timeLen, latLen,lonLen), dateStr)
      val KVStoreEnd=System.currentTimeMillis()
      println("入库结束:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreEnd)+";"+bakMsg)

      helper.close()
      println("入库关闭:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis())+";"+bakMsg)



      val localIP:String=SparkUtil.getIp()
      if(retCode0>0){
        WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"groupKey="+groupKey+";timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }else{
        WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"groupKey="+groupKey+";timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }

      (retCode0,"")
    }

    //----------------------------入库日志采集------------------------------
    val ipStr=SparkUtil.getIp(this.metaurl)
    var basicRemark:JSONObject=new JSONObject
    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    basicRemark=AttributesOperation.addAsyOpen(basicRemark)
    basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
    basicRemark=AttributesOperation.addAppId(basicRemark,appID)
    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
    val (usableStr:String,legalStr:String)={
      if(retCode <0){
        ("N","N")
      }else{
        ("Y","Y")
      }
    }
    var stateRemark:JSONObject=new JSONObject()
    stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
    SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
    //----------------------------------------------------------------------------

    if(retCode <0){
      false
    }else{
      true
    }

  }*/
  /*private def rddInputKV1(obsdate:Date,idNameTimeStepArrScidatas:(Any,Iterable[SciDataset]),stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String): Boolean ={
    val groupKey=idNameTimeStepArrScidatas._1
    val arrScidatas:Iterable[SciDataset]=idNameTimeStepArrScidatas._2

    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val GS:SciDataset=arrScidatas.last
    val idName:String=AttributesOperation.getIDName(GS)
    val timeStep:Double=AttributesOperation.gettimeStep(GS)

    val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

    val (timeV,heightV,latV,lonV)=SciDatasetOperation.getVars(GS,timeName,heightName,latName,lonName)

    val time=timeV.data
    val height=heightV.data
    val latSplit=DataFormatUtil.doubleFormat(latV.data)
    val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

    val latlonStep:Double=0.01d
    val (lat,lon)=PropertiesUtil.getLatLon(latlonStep)

    val latIndex=ArrayOperation.HashMapSearch(lat,latSplit(0))
    val lonIndex=ArrayOperation.HashMapSearch(lon,lonSplit(0))

    val timeLen=time.length
    val heightLen=height.length
    val latLen=latSplit.length
    val lonLen=lonSplit.length


    val types:String =  PropertiesUtil.getKVStoreType(timeStep,idName)


    val (retCode:Long,retStr:String)={
      val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
      val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
      val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)


      val realHost:String={
        try{
          val host_ports:Array[(String,String,Int,String)]=KVStoreConf.gethost_port(this.metaurl,types)
          host_ports.map(f=>(f._1+"="+f._2+":"+f._3)).reduce((x,y)=>(x+";"+y))
        }catch {
          case e:Exception=>{
            e.printStackTrace()
            ""
          }
        }
      }

      val bakMsg="设置入库地址="+this.metaurl+";设置等待时间="+this.timeout+"ms;入库reftime="+dateStr+";入库Key="+types+";"+groupKey+";实际库地址:"+realHost

      val hashdata:util.HashMap[String, AnyRef]=new util.HashMap[String, AnyRef]
      arrScidatas.foreach(
        scidata=>{
          val fcType=AttributesOperation.getFcType(scidata)

          //输出入库前nc //入库前站点抽查
          if(this.inputKVStoreOutOpen || this.checkStationOpen){
            val timeStep0={
              if(timeStep==12.0d){
                timeStep * 2
              }else{
                timeStep
              }
            }
            val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

            if(this.inputKVStoreOutOpen){
              println("输出入库前数据开关(pom.inputKVStoreOut.Open)开启")
              val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
              WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
              println(groupKey+";fcType="+fcType+"输出;输出地址为:"+inputKVStorePath)
            }else{
              println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
            }

            if(this.checkStationOpen){
              println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
              val stationinputKV=stationsInfos._2
              val stationsInfo=stationsInfos._1
              CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,fcdate,fcdate)
            }else{
              println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
            }
          }

          val dataNames:Array[String]={
            if(timeStep==1){
              PropertiesUtil.getwElement(fcType,timeStep)
            }else if(timeStep==12){
              val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
              WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
            }else{
              val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
              null
            }
          }

          dataNames.foreach(
            dataName=>{
              val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
              val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

              hashdata.put(key,arr)
              val nowStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr1(new Date())
              println("入库数据准备:"+nowStr+",idName="+idName+",timeStep="+timeStep+",data.size="+hashdata.size()+";"+ShowUtil.MapShowKeys(hashdata))+";"+groupKey
            }
          )

        }
      )

      println(groupKey+";rddInputKV1,hashdata.size="+hashdata.size)




      val helper = KVHelper.KVHelper(this.metaurl,types,this.timeout)

      println("入库连接中:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis())+"......"+bakMsg)
      helper.open()

      val KVStoreStart=System.currentTimeMillis()
      println("入库开始:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreStart)+";"+bakMsg)
      val retCode0:Long=helper.writeWithException(latIndex, lonIndex, hashdata, Array[Int](timeLen, latLen,lonLen), dateStr)
      val KVStoreEnd=System.currentTimeMillis()
      println("入库结束:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(KVStoreEnd)+";"+bakMsg)

      helper.close()
      println("入库关闭:"+DataFormatUtil.YYYYMMDDHHMMSSStr1(System.currentTimeMillis())+";"+bakMsg)









      val localIP:String=SparkUtil.getIp()
      if(retCode0>0){
        WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }else{
        WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+",hashdata.size="+hashdata.size()+";localIP="+localIP+";retCode0="+retCode0+";"+bakMsg)
      }

      (retCode0,"")
    }

    //----------------------------入库日志采集------------------------------
    val ipStr=SparkUtil.getIp(this.metaurl)
    var basicRemark:JSONObject=new JSONObject
    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    basicRemark=AttributesOperation.addAsyOpen(basicRemark)
    basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
    basicRemark=AttributesOperation.addAppId(basicRemark,appID)
    basicRemark=AttributesOperation.addIDName(basicRemark,idName)
    basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
    val (usableStr:String,legalStr:String)={
      if(retCode <0){
        ("N","N")
      }else{
        ("Y","Y")
      }
    }
    var stateRemark:JSONObject=new JSONObject()
    stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
    SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
    //----------------------------------------------------------------------------
    if(retCode <0){
      false
    }else{
      true
    }
  }*/

  /*private def rdd2Array3_Return(obsdate:Date, arrRdd:Iterable[RDD[SciDataset]], stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String,idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): Boolean ={
    val Rdds: RDD[(String,Iterable[((String,Double),Iterable[SciDataset])])]=MatchSciDatas.matchByName3(idNameMaps,arrRdd)
    //    val Rdds: RDD[((String,Double),Iterable[SciDataset])]=MatchSciDatas.matchByName2(arrRdd)
    println("rdd2Array3_Return,arrRDD.length="+arrRdd.size)

    val flag:Boolean=Rdds.map(
      scidatas=>{
        val groupKey=scidatas._1
        val groupRdd=scidatas._2
        print("groupKey="+groupKey+"开始,groupSize="+groupRdd.size)
        val flag:Boolean=groupRdd.map(f=>{
          /*val idName=f._1._1
          val timeStep=f._1._2*/
          val scidatas=f._2
          this.rddInputKV0(obsdate,(groupKey,scidatas),stationsInfos,appID)
        }).reduce((x,y)=>(x && y))
        println("rdd2Array3_Return,arrRDD.length="+arrRdd.size+":"+groupKey+" Done."+flag)
        flag
      }
    ).collect().reduce((x,y)=>(x && y))

    flag
  }*/
  def rdd2Array3_Action(obsdate:Date, arrRdd:Iterable[RDD[SciDatasetCW]], stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String,idNameMaps:mutable.HashMap[String,Array[(String,Double)]],jsonFile:String): Unit ={
    val Rdds: RDD[(String,Iterable[((String,Double),Iterable[SciDatasetCW])])]=MatchSciDatasCW.matchByName3(idNameMaps,arrRdd,jsonFile)
    //    val Rdds: RDD[((String,Double),Iterable[SciDataset])]=MatchSciDatas.matchByName2(arrRdd)
    println("rdd2Array3_Action,arrRDD.length="+arrRdd.size)
    Rdds.foreach(scidatas=>{
      val groupKey=scidatas._1
      val groupRdd=scidatas._2
      print("groupKey="+groupKey+"开始,groupSize="+groupRdd.size)
      val flag:Boolean=groupRdd.map(f=>{
        /*val idName=f._1._1
        val timeStep=f._1._2*/
        val scidatas=f._2
        this.rddInputKV(obsdate,(groupKey,scidatas),stationsInfos,appID)
      }).reduce((x,y)=>(x && y))
      println("rdd2Array3_Return,arrRDD.length="+arrRdd.size+":"+groupKey+" Done."+flag)
      //      flag
    })
  }

  /*private def rdd2Array4_Return(obsdate:Date, arrRdd:Array[RDD[SciDataset]], stationsInfos:(mutable.HashMap[String,String],String,Date)): Boolean ={
    val Rdds: RDD[((String,Double),Iterable[SciDataset])]=MatchSciDatas.matchByName2(arrRdd)

    println("rdd2Array4_Return,arrRDD.length="+arrRdd.length)


    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val appID=ContextUtil.getApplicationID

    val flag:Boolean=Rdds.map(
      scidatas=>{
        val idName:String=scidatas._1._1
        val timeStep:Double=scidatas._1._2
        val scidataIte=scidatas._2

        //        val groupKeyStr=MatchSciDatas.idNameTimeStep2KeyShowStr(idName,timeStep)

        val GS:SciDataset=scidataIte.last

        val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

        val (timeV,heightV,latV,lonV)=VO.getVars(GS,timeName,heightName,latName,lonName)

        val time=timeV.data
        val height=heightV.data
        val latSplit=DataFormatUtil.doubleFormat(latV.data)
        val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

        val (lat,lon)=PropertiesUtil.getLatLon()

        val latIndex=AO.HashMapSearch(lat,latSplit(0))
        val lonIndex=AO.HashMapSearch(lon,lonSplit(0))

        val timeLen=time.length
        val heightLen=height.length
        val latLen=latSplit.length
        val lonLen=lonSplit.length

        val dataArr:Iterable[util.HashMap[String, AnyRef]]=scidataIte.map(
          scidata=>{
            val fcType=AttributesOperation.getFcType(scidata)

            //输出入库前nc //入库前站点抽查
            if(this.inputKVStoreOutOpen || this.checkStationOpen){
              val timeStep0={
                if(timeStep==12.0d){
                  timeStep * 2
                }else{
                  timeStep
                }
              }
              val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

              if(this.inputKVStoreOutOpen){
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)开启")
                val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
                WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
                println("idName="+idName+",timeStep="+timeStep+",fcType="+fcType+"输出;输出地址为:"+inputKVStorePath)
              }else{
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
              }

              if(this.checkStationOpen){
                println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
                val stationinputKV=stationsInfos._2
                val stationsInfo=stationsInfos._1
                CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,fcdate,fcdate)
              }else{
                println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
              }
            }




            val dataNames:Array[String]={
              if(timeStep==1){
                PropertiesUtil.getwElement(fcType,timeStep)
              }else if(timeStep==12){
                val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
                WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
              }else{
                val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
                null
              }
            }

            val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
              dataName=>{
                val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, Object]
                val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
                val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

                data00.put(key,arr)
                data00
              }
            )
            val data0:util.HashMap[String, AnyRef]=dataarr0.reduce(
              (x,y)=>{
                x.putAll(y)
                x
              }
            )
            data0
          }
        )
        val data:util.HashMap[String, AnyRef]=dataArr.reduce(
          (x,y)=>{
            x.putAll(y)
            x
          }
        )

        val types:String =  PropertiesUtil.getKVStoreType(timeStep)
        val helper = new KVStoreSimpleHelper(this.metaurl,types,this.timeout)


        //        val dateStr:String=WeatherDate.ObsGetKVDateStr_Old(obsdate,timeStep)
        val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
        val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
        val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

        val (retCode:Long,retStr:String)={
          //          try{
          helper.open()

          val KVStoreStart=System.currentTimeMillis()
          val retCode0=helper.writeWithException(latIndex, lonIndex, data, Array[Int](timeLen, latLen,lonLen), dateStr)
          helper.close()
          val KVStoreEnd=System.currentTimeMillis()

          val localIP:String=SparkUtil.getIp()
          if(retCode0>0){
            WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }else{
            WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }

          (retCode0,"")
          /* }catch{
             case e:Exception=>{
               ( -1 ,e.toString)
             }
           }*/
        }

        //----------------------------入库日志采集------------------------------
        val ipStr=SparkUtil.getIp(this.metaurl)
        var basicRemark:JSONObject=new JSONObject
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addAsyOpen(basicRemark)
        basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
        basicRemark=AttributesOperation.addAppId(basicRemark,appID)
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
        val (usableStr:String,legalStr:String)={
          if(retCode <0){
            ("N","N")
          }else{
            ("Y","Y")
          }
        }
        var stateRemark:JSONObject=new JSONObject()
        stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
        SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
        //----------------------------------------------------------------------------

        if(retCode <0){
          false
        }else{
          true
        }

      }
    ).reduce((x,y)=>(x && y))

    flag
  }
  private def rdd2Array4_Action(obsdate:Date, arrRdd:Array[RDD[SciDataset]], stationsInfos:(mutable.HashMap[String,String],String,Date)): Unit ={
    val Rdds: RDD[((String,Double),Iterable[SciDataset])]=MatchSciDatas.matchByName2(arrRdd)

    println("rdd2Array4,arrRDD.length="+arrRdd.length)


    val fcdate=WeatherDate.ObsGetFc(obsdate)

    val appID=ContextUtil.getApplicationID

    Rdds.foreach(
      scidatas=>{
        val idName:String=scidatas._1._1
        val timeStep:Double=scidatas._1._2
        val scidataIte:Iterable[SciDataset]=scidatas._2

        //        val groupKeyStr=MatchSciDatas.idNameTimeStep2KeyShowStr(idName,timeStep)

        val GS:SciDataset=scidataIte.last

        val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

        val (timeV,heightV,latV,lonV)=VO.getVars(GS,timeName,heightName,latName,lonName)

        val time=timeV.data
        val height=heightV.data
        val latSplit=DataFormatUtil.doubleFormat(latV.data)
        val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

        val (lat,lon)=PropertiesUtil.getLatLon()

        val latIndex=AO.HashMapSearch(lat,latSplit(0))
        val lonIndex=AO.HashMapSearch(lon,lonSplit(0))

        val timeLen=time.length
        val heightLen=height.length
        val latLen=latSplit.length
        val lonLen=lonSplit.length

        //        arrScidatas.reduce

        val dataArr:Iterable[util.HashMap[String, AnyRef]]=scidataIte.map(
          scidata=>{
            val fcType=AttributesOperation.getFcType(scidata)

            //输出入库前nc //入库前站点抽查
            if(this.inputKVStoreOutOpen || this.checkStationOpen){
              val timeStep0={
                if(timeStep==12.0d){
                  timeStep * 2
                }else{
                  timeStep
                }
              }
              val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

              if(this.inputKVStoreOutOpen){
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)开启")
                val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
                WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
                println("idName="+idName+",timeStep="+timeStep+",fcType="+fcType+"输出;输出地址为:"+inputKVStorePath)
              }else{
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
              }

              if(this.checkStationOpen){
                println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
                val stationinputKV=stationsInfos._2
                val stationsInfo=stationsInfos._1
                CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,fcdate,fcdate)
              }else{
                println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
              }
            }




            val dataNames:Array[String]={
              if(timeStep==1){
                PropertiesUtil.getwElement(fcType,timeStep)
              }else if(timeStep==12){
                val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
                WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
              }else{
                val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
                null
              }
            }

            val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
              dataName=>{
                val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, Object]
                val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
                val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

                data00.put(key,arr)
                data00
              }
            )
            val data0:util.HashMap[String, AnyRef]=dataarr0.reduce(
              (x,y)=>{
                x.putAll(y)
                x
              }
            )
            data0
          }
        )
        val data:util.HashMap[String, AnyRef]=dataArr.reduce(
          (x,y)=>{
            x.putAll(y)
            x
          }
        )

        val types:String =  PropertiesUtil.getKVStoreType(timeStep)
        val helper = new KVStoreSimpleHelper(this.metaurl,types,this.timeout)


        //        val dateStr:String=WeatherDate.ObsGetKVDateStr_Old(obsdate,timeStep)
        val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
        val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
        val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

        val (retCode:Long,retStr:String)={
          //          try{
          helper.open()

          val KVStoreStart=System.currentTimeMillis()
          val retCode0=helper.writeWithException(latIndex, lonIndex, data, Array[Int](timeLen, latLen,lonLen), dateStr)
          helper.close()
          val KVStoreEnd=System.currentTimeMillis()

          val localIP:String=SparkUtil.getIp()
          if(retCode0>0){
            WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }else{
            WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }

          (retCode0,"")
          /* }catch{
             case e:Exception=>{
               ( -1 ,e.toString)
             }
           }*/
        }

        //----------------------------入库日志采集------------------------------
        val ipStr=SparkUtil.getIp(this.metaurl)
        var basicRemark:JSONObject=new JSONObject
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addAsyOpen(basicRemark)
        basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
        basicRemark=AttributesOperation.addAppId(basicRemark,appID)
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
        val (usableStr:String,legalStr:String)={
          if(retCode <0){
            ("N","N")
          }else{
            ("Y","Y")
          }
        }
        var stateRemark:JSONObject=new JSONObject()
        stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
        SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
        //----------------------------------------------------------------------------

      }
    )
  }
  private def rdd2Array5_Return(obsdate:Date, arrRdd:Array[RDD[SciDataset]], stationsInfos:(mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): Boolean ={
    val Rdds: RDD[(String,Iterable[SciDataset])]=MatchSciDatas.matchByName3(idNameMaps,arrRdd)

    println("rdd2Array5_Return,arrRDD.length="+arrRdd.length)


    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val appID=ContextUtil.getApplicationID



    val flag:Boolean=Rdds.map(
      scidatas=>{

        val scidataIte=scidatas._2

        val GS:SciDataset=scidataIte.last
        val idName:String=AttributesOperation.getIDName(GS)
        val timeStep:Double=AttributesOperation.gettimeStep(GS)

        val groupKeyStr=MatchSciDatas.groupKeyShowStr(idNameMaps,idName,timeStep)

        val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

        val (timeV,heightV,latV,lonV)=VO.getVars(GS,timeName,heightName,latName,lonName)

        val time=timeV.data
        val height=heightV.data
        val latSplit=DataFormatUtil.doubleFormat(latV.data)
        val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

        val (lat,lon)=PropertiesUtil.getLatLon()

        val latIndex=AO.HashMapSearch(lat,latSplit(0))
        val lonIndex=AO.HashMapSearch(lon,lonSplit(0))

        val timeLen=time.length
        val heightLen=height.length
        val latLen=latSplit.length
        val lonLen=lonSplit.length

        val dataArr:Iterable[util.HashMap[String, AnyRef]]=scidataIte.map(
          scidata=>{
            val fcType=AttributesOperation.getFcType(scidata)

            //输出入库前nc //入库前站点抽查
            if(this.inputKVStoreOutOpen || this.checkStationOpen){
              val timeStep0={
                if(timeStep==12.0d){
                  timeStep * 2
                }else{
                  timeStep
                }
              }
              val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

              if(this.inputKVStoreOutOpen){
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)开启")
                val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
                WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
                println(groupKeyStr+";fcType="+fcType+"输出;输出地址为:"+inputKVStorePath)
              }else{
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
              }

              if(this.checkStationOpen){
                println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
                val stationinputKV=stationsInfos._2
                val stationsInfo=stationsInfos._1
                CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,fcdate,fcdate)
              }else{
                println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
              }
            }




            val dataNames:Array[String]={
              if(timeStep==1){
                PropertiesUtil.getwElement(fcType,timeStep)
              }else if(timeStep==12){
                val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
                WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
              }else{
                val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
                null
              }
            }

            val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
              dataName=>{
                val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, Object]
                val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
                val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

                data00.put(key,arr)
                data00
              }
            )
            val data0:util.HashMap[String, AnyRef]=dataarr0.reduce(
              (x,y)=>{
                x.putAll(y)
                x
              }
            )
            data0
          }
        )
        val data:util.HashMap[String, AnyRef]=dataArr.reduce(
          (x,y)=>{
            x.putAll(y)
            x
          }
        )

        val types:String =  PropertiesUtil.getKVStoreType(timeStep)
        val helper = new KVStoreSimpleHelper(this.metaurl,types,this.timeout)


        //        val dateStr:String=WeatherDate.ObsGetKVDateStr_Old(obsdate,timeStep)
        val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
        val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
        val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

        val (retCode:Long,retStr:String)={
          //          try{
          helper.open()

          val KVStoreStart=System.currentTimeMillis()
          val retCode0=helper.writeWithException(latIndex, lonIndex, data, Array[Int](timeLen, latLen,lonLen), dateStr)
          helper.close()
          val KVStoreEnd=System.currentTimeMillis()

          val localIP:String=SparkUtil.getIp()
          if(retCode0>0){
            WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }else{
            WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }

          (retCode0,"")
          /* }catch{
             case e:Exception=>{
               ( -1 ,e.toString)
             }
           }*/
        }

        //----------------------------入库日志采集------------------------------
        val ipStr=SparkUtil.getIp(this.metaurl)
        var basicRemark:JSONObject=new JSONObject
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addAsyOpen(basicRemark)
        basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
        basicRemark=AttributesOperation.addAppId(basicRemark,appID)
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
        val (usableStr:String,legalStr:String)={
          if(retCode <0){
            ("N","N")
          }else{
            ("Y","Y")
          }
        }
        var stateRemark:JSONObject=new JSONObject()
        stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
        SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
        //----------------------------------------------------------------------------

        if(retCode <0){
          false
        }else{
          true
        }

      }
    ).reduce((x,y)=>(x && y))

    flag
  }
  private def rdd2Array5_Action(obsdate:Date, arrRdd:Array[RDD[SciDataset]], stationsInfos:(mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): Unit ={
    val Rdds: RDD[(String,Iterable[SciDataset])]=MatchSciDatas.matchByName3(idNameMaps,arrRdd)

    println("rdd2Array5_Action,arrRDD.length="+arrRdd.length)

    val fcdate=WeatherDate.ObsGetFc(obsdate)
    val appID=ContextUtil.getApplicationID

    /*//分区测试
    val tmppath0=HDFSFileUtil.tohdfsfile("/data/dataSource/test/"+appID+"/")
    Rdds.saveAsObjectFile(tmppath0)*/

    Rdds.foreach(
      scidatas=>{

        val scidataIte=scidatas._2

        val GS:SciDataset=scidataIte.last
        val idName:String=AttributesOperation.getIDName(GS)
        val timeStep:Double=AttributesOperation.gettimeStep(GS)

        val groupKeyStr=MatchSciDatas.groupKeyShowStr(idNameMaps,idName,timeStep)

        val (timeName, heightName, latName, lonName)=AttributesOperation.getVarName(GS)

        val (timeV,heightV,latV,lonV)=VO.getVars(GS,timeName,heightName,latName,lonName)

        val time=timeV.data
        val height=heightV.data
        val latSplit=DataFormatUtil.doubleFormat(latV.data)
        val lonSplit=DataFormatUtil.doubleFormat(lonV.data)

        val (lat,lon)=PropertiesUtil.getLatLon()

        val latIndex=AO.HashMapSearch(lat,latSplit(0))
        val lonIndex=AO.HashMapSearch(lon,lonSplit(0))

        val timeLen=time.length
        val heightLen=height.length
        val latLen=latSplit.length
        val lonLen=lonSplit.length

        val dataArr:Iterable[util.HashMap[String, AnyRef]]=scidataIte.map(
          scidata=>{
            val fcType=AttributesOperation.getFcType(scidata)

            //输出入库前nc //入库前站点抽查
            if(this.inputKVStoreOutOpen || this.checkStationOpen){
              val timeStep0={
                if(timeStep==12.0d){
                  timeStep * 2
                }else{
                  timeStep
                }
              }
              val scidata0=AttributesOperation.updateTimeStepPlusName(scidata,timeStep0)

              if(this.inputKVStoreOutOpen){
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)开启")
                val inputKVStorePath:String=PropertiesUtil.getinputKVStorePath(timeStep,fcType,obsdate)
                WriteFile.WriteNcFile_addID_tmp(scidata0,inputKVStorePath,appID)
                println(groupKeyStr+";fcType="+fcType+"输出;输出地址为:"+inputKVStorePath)
              }else{
                println("输出入库前数据开关(pom.inputKVStoreOut.Open)关闭:不输出输入前数据")
              }

              if(this.checkStationOpen){
                println("输出入库前数据站点抽查(pom.checkStation.Open)开启")
                val stationinputKV=stationsInfos._2
                val stationsInfo=stationsInfos._1
                CheckStation.checkStation(appID,stationsInfo,stationinputKV,scidata0,fcdate,fcdate)
              }else{
                println("输出入库前数据站点抽查(pom.checkStation.Open)关闭:不输出输入前数据")
              }
            }




            val dataNames:Array[String]={
              if(timeStep==1){
                PropertiesUtil.getwElement(fcType,timeStep)
              }else if(timeStep==12){
                val dataNames_alTypetimeIAs=PropertiesUtil.getdataNames_alTypetimeIAs_from12h(fcType,timeStep)
                WeatherInterpolation.getnewdataName(dataNames_alTypetimeIAs)
              }else{
                val e:Exception=new Exception("timeStep="+timeStep+"没有入库操作")
                null
              }
            }

            val dataarr0:Array[util.HashMap[String, AnyRef]]=dataNames.map(
              dataName=>{
                val data00: util.HashMap[String, AnyRef] = new util.HashMap[String, Object]
                val (arr:Array[Array[Array[Double]]],arrLen:Array[Int])=this.sci2Array3(scidata,timeName,heightName,latName,lonName,dataName)
                val key:String=PropertiesUtil.getKVStoreKey(dataName,timeStep)

                data00.put(key,arr)
                data00
              }
            )
            val data0:util.HashMap[String, AnyRef]=dataarr0.reduceLeft(
              (x,y)=>{
                x.putAll(y)
                x
              }
            )
            data0
          }
        )
        val data:util.HashMap[String, AnyRef]=dataArr.reduceLeft(
          (x,y)=>{
            x.putAll(y)
            x
          }
        )

        val types:String =  PropertiesUtil.getKVStoreType(timeStep)
        val helper = new KVStoreSimpleHelper(this.metaurl,types,this.timeout)


        //        val dateStr:String=WeatherDate.ObsGetKVDateStr_Old(obsdate,timeStep)
        val dateStr:String=WeatherDate.ObsGetKVDateStr_New(obsdate,timeStep)
        val olddateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(WeatherDate.KVDateStrGetObs_New(dateStr,timeStep))
        val obsdateStr:String=DataFormatUtil.YYYYMMDDHHMMSSStr0(obsdate)

        val (retCode:Long,retStr:String)={
          //          try{
          helper.open()

          val KVStoreStart=System.currentTimeMillis()
          val retCode0=helper.writeWithException(latIndex, lonIndex, data, Array[Int](timeLen, latLen,lonLen), dateStr)
          helper.close()
          val KVStoreEnd=System.currentTimeMillis()

          val localIP:String=SparkUtil.getIp()
          if(retCode0>0){
            WeatherShowtime.showDateStrOut1("入库成功",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }else{
            WeatherShowtime.showDateStrOut1("入库失败",KVStoreStart,KVStoreEnd,"timeStep="+timeStep+";idName="+idName+";dateStr="+dateStr+"("+olddateStr+"/"+obsdateStr+");(latIndex="+latIndex+",lonIndex="+lonIndex+");timeLen="+timeLen+",latLen="+latLen+",lonLen="+lonLen+";localIP="+localIP+";retCode0="+retCode0)
          }

          (retCode0,"")
          /* }catch{
             case e:Exception=>{
               ( -1 ,e.toString)
             }
           }*/
        }

        //----------------------------入库日志采集------------------------------
        val ipStr=SparkUtil.getIp(this.metaurl)
        var basicRemark:JSONObject=new JSONObject
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addAsyOpen(basicRemark)
        basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
        basicRemark=AttributesOperation.addAppId(basicRemark,appID)
        basicRemark=AttributesOperation.addIDName(basicRemark,idName)
        basicRemark=AttributesOperation.addbasicProcess(basicRemark,"入库"+this.getClass.getName)
        val (usableStr:String,legalStr:String)={
          if(retCode <0){
            ("N","N")
          }else{
            ("Y","Y")
          }
        }
        var stateRemark:JSONObject=new JSONObject()
        stateRemark=AttributesOperation.addOtherMark(stateRemark,"targetIP:"+ipStr+";retCode="+retCode+";retStr="+retStr)
        SendMsg.serviceSend(basicRemark,usableStr,legalStr,stateRemark)
        //----------------------------------------------------------------------------

      }
    )
  }*/

  def ArrScidata2Array(obsdate:Date, idNameTimeStepArrScidatas:((String,Double),Iterable[SciDatasetCW]), stationsInfos:(mutable.HashMap[String,String],String,Date),appID:String,idNameMaps:mutable.HashMap[String,Array[(String,Double)]]): Boolean={
    this.rddInputKV(obsdate,idNameTimeStepArrScidatas,stationsInfos,appID)
    //    this.rddInputKV1(obsdate,idNameTimeStepArrScidatas,stationsInfos,appID)
  }

  private def updateTimeStepPlusName (scidata: SciDatasetCW, timeStep: Double): SciDatasetCW = {
    val scidata0 = AttributesOperationCW.updateTimeStep(scidata, timeStep)
    val fcType = AttributesOperationCW.getFcType(scidata0)
    val idName = AttributesOperationCW.getIDName(scidata0)
    val newName = SplitMatchInfo.oldName2newName(fcType, idName, timeStep)
    scidata0.setName(newName)
    scidata0
  }

  /*private def RddArrayScidata2Array(obsdate:Date, RddArrScidatas:RDD[(String,Iterable[((String,Double),Iterable[SciDataset])])], stationsInfos:(mutable.HashMap[String,String],String,Date),idNameMaps:mutable.HashMap[String,Array[(String,Double)]],appID:String): Unit ={
    RddArrScidatas.foreach(f=>{
      val fcdate=WeatherDate.ObsGetFc(obsdate)
      val key=f._1
      val ite0:Iterable[((String,Double),Iterable[SciDataset])]=f._2
      ite0.foreach(ite1=>{
        this.ArrScidata2Array(obsdate, ite1, stationsInfos,appID,idNameMaps)
      })
    })
  }*/
  def main(args:Array[String]): Unit ={
    /*val appID=ContextUtil.getApplicationID
    val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
    val jsonRdd=PropertiesUtil.getJsonRdd
    val dateStr=args(0)
    val timeStep=args(1).toDouble
    val timeSteps=Array(1.0d,12.0d)

    //N1H N12H

    val date:Date=DataFormatUtil.YYYYMMDDHHMMSS0(dateStr)

    val FcTypes=Array(ConstantUtil.PRE,ConstantUtil.TEM,ConstantUtil.WIND,ConstantUtil.CLOUD,ConstantUtil.RHU,ConstantUtil.WEATHER)

    //    val paths=FcTypes.map(fctype=>PropertiesUtil.getfcPath(timeStep,fctype,dateStr))

    val times={
      if(timeStep==1.0d){
        ArrayOperation.ArithmeticArray(1.0d,24.0d,1.0d)
      }else{
        null
      }
    }

    val outpath=PropertiesUtil.getfcPath(timeStep,"FuseTest",date)

    val arrRdd:Array[RDD[SciDataset]]=FcTypes.map(
      fctype=> {
        val dataNames=PropertiesUtil.getrElement_rename(fctype,timeStep)
        val path=PropertiesUtil.getfcPath(timeStep,fctype,date)
        val rdd:RDD[SciDataset]=ReadFile.ReadFcRdd(jsonRdd,timeStep,fctype,date)
        val wrdd=DataInterception.datacutByTime(rdd,dataNames,times)
        rdd
      }
    )

    val Rdds=MatchSciDatas.fuse0(arrRdd,timeName,heightName,latName,lonName)

    WriteFile.WriteNcFile_addID_tmpAction(Rdds,outpath,appID)
    val idNameMaps:mutable.HashMap[String,Array[(String,Double)]]=MatchSciDatas.getidNameMap(timeSteps)
    val stationsInfo=ReadFile.ReadStationInfoMap_IdGetlatlon0()
    val stationinputKV=PropertiesUtil.stationinputKV
    val stationsInfos=(stationsInfo,stationinputKV,date)
    this.rdd2Array3_Return(date,arrRdd,stationsInfos,appID,idNameMaps)*/
  }
}
