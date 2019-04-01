package com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil

import java.util.Date

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.operation.{ArrayOperation, DateOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil.ContextUtil
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

//import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

/**
  * created by lijing on 2018/4/1
  * edited by wufenqiang on 2018/4/2
  */
object NcRevisedByOcfCW {
  private val defaultStation:String="99999"
  private val defaultV:Double= -999.9d
  private val timeLimit:Double=168.0d

  def RevisedByCity(interRdd: RDD[SciDatasetCW], FcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap:Map[String, Map[String, Map[String, Float]]]) : RDD[SciDatasetCW] = {
    val sc=ContextUtil.getSparkContext()
    val sciGeoValue=sc.broadcast(sciGeo)
    //    val ocfMapValue=sc.broadcast(ocfMap)
    //读取拆分后的NMC预报RDD
    interRdd.map(
      SciData => {
        this.RevisedByCity(SciData,FcType,ocfdate,timeStep,sciGeoValue.value,ocfMap)
      })
  }
  def RevisedByCity(scidata: SciDatasetCW, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap:Map[String, Map[String, Map[String, Float]]]): SciDatasetCW ={
    this.RevisedByCity3(scidata: SciDatasetCW, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap)
  }

  //  //仅省会城市做ocf订正覆盖
  //  private def RevisedByCity0(scidata: SciDataset, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDataset, ocfMap:Map[String, Map[String,Map[String,Double]]]): SciDataset ={
  //    val idName=AttributesOperation.getIDName(scidata)
  //    //    val eleName=fcType
  //    val dataNames=PropertiesUtil.getrElement_rename(fcType,timeStep)
  //
  //    val fcdate=AttributesOperation.getfcdate(scidata)
  //    val fcdateStr=DateFormatUtil.YYYYMMDDHHStr0(fcdate)
  //
  //
  //    //fcdate一致性测试代码
  //    {
  //      val fcdate0=WeatherDate.OcfGetFc(ocfdate)
  //      if(!DateOperation.equal(fcdate0,fcdate)){
  //        val e:Exception=new Exception("RevisedByCity0接口中fcdate一致性有误")
  //        e.printStackTrace()
  //      }
  //    }
  //    /*
  //        val TimRes:String={
  //          if(timeStep == 1.0d){
  //            "fc_1h"
  //          }else if(timeStep == 3.0d){
  //            "fc_3h"
  //          }else if(timeStep == 12.0d){
  //            "fc_12h"
  //          }else{
  //            val e:Exception=new Exception("timeStep输入错误,没有对应的方法,timeStep="+timeStep)
  //            e.printStackTrace()
  //            null
  //          }
  //        }*/
  //
  //    //    val dataName:String= prop.getProperty(eleName).split(",")(1)
  //    //    val ocfMap:Map[String, Map[String,Map[String,Double]]] = ReadFile.ReadOcfFile(ocfPath,timeStep)
  //    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
  //    val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  //    val cal:Calendar=Calendar.getInstance()
  //
  //    val vals=new ArrayBuffer[Variable]
  //
  //    //获取变量名
  //    val (timeName,heightName,latName,lonName)=PropertiesUtil.varNames
  //
  //    val timeV=scidata.apply(timeName)
  //    val heightV=scidata.apply(heightName)
  //    val latV=scidata.apply(latName)
  //    val lonV=scidata.apply(lonName)
  //
  //    vals+= timeV+= heightV+= latV+= lonV
  //
  //    //获取数据维度大小信息
  //    val timeDims = scidata.apply(timeName).shape()(1)
  //    val heightDims =scidata.apply(heightName).shape()(1)
  //    val latDims = scidata.apply(latName).shape()(1)
  //    val lonDims = scidata.apply(lonName).shape()(1)
  //    val length = timeDims * heightDims * latDims * lonDims
  //    val planeLength = latDims * lonDims * heightDims
  //    val dims = List((timeName, timeDims), (heightName, heightDims), (latName, latDims), (lonName, lonDims))
  //    val shape = Array(timeDims, heightDims, latDims, lonDims)
  //    val beginTim = scidata.apply(timeName).data().head
  //    val endTim = scidata.apply(timeName).data().last
  //    //      val timeStep = (endTim - beginTim) / (timeDims - 1)
  //    val beginLat = scidata.apply(latName).data().head
  //    val endLat = scidata.apply(latName).data().last
  //    val beginLon = scidata.apply(lonName).data().head
  //    val endLon = scidata.apply(lonName).data().last
  //    val spaceStep = (endLon - beginLon) / (lonDims - 1)
  //    println(" latBegein + "+beginLat+" latEnd + "+endLat+" lonBegein + "+beginLon+" lonEnd +"+ endLon)
  //    //标记拆分区域内每个格点对应的ocf预报站号，没有对应关系的用代码-999表示，注意该数组是平面二维
  //    val flagArray = this.StationArr0(sciGeo, beginLat, endLat, beginLon, endLon, spaceStep)
  //
  //
  //    val suvals=dataNames.map(
  //      dataName=>{
  //
  //        val ele:String={
  //          if(dataName.endsWith("Max")){
  //            fcType+"_Max"
  //          }else if(dataName.endsWith("Min")){
  //            fcType+"_Min"
  //          }else {
  //            fcType
  //          }
  //        }
  //
  //        //获取NMC要素预报数据
  //        val DataV=scidata.apply(dataName)
  //        val Data = DataV.data()
  //        //获取拆分区域的边界点和时空分辨率
  //
  //        //val flagArray = this.ReadGeoData(lat0, lon0, latDims, lonDims, level, distance, forecast_id,beginLat,endLat, beginLon, endLon, spaceStep)
  //        //  println("++++行政区域大小计算完毕:++++ "+    flagArray.count())
  //
  //        for(i <- 0 until planeLength) {
  //          //   RevisedData(i) = Data(i)
  //          val citySta = flagArray(i%(latDims * lonDims * heightDims)).toInt
  //          /*if(i==238230){
  //             println(citySta)
  //           }*/
  //          if(citySta != -999) { //如果地理和行政区域关系上有和县级站点的对应关系
  //            if(ocfMap.contains(citySta.toString)){ //如果ocf文件中存在该县级站站号
  //
  //              for(t <- 0 until timeDims){
  //                val tstep =(timeStep * (t+1)).toInt
  //                if(tstep<=this.timeLimit){
  //                  cal.setTime(dateFormat.parse(fcdateStr))
  //                  cal.add(Calendar.HOUR_OF_DAY,tstep) //计算每个格点的nmc数据对应的预报时间
  //                  val time: String =dateFormat.format(cal.getTime)
  //                  if(ocfMap.get(citySta.toString).last.contains(time)){ //如果ocf文件中存在和nmc匹配的预报时间
  //                    if(ocfMap.get(citySta.toString).last.get(time).last.contains(ele)) { //如果ocf文件中存在nmc订正需要的要素
  //                      Data(i + t * planeLength) = ocfMap.get(citySta.toString).get(time).get(ele).last //则用ocf文件的数据覆盖nmc的格点数据
  //                      /*if (citySta.toString == "54511") {
  //                        println("关联站号： "+citySta+"积分步长： "+tstep+ " 时间： "+time+"  "+"订正值： "+Data(i + t*planeLength))
  //                      }*/
  //                    }
  //                  }
  //                }
  //              }
  //            }
  //          }
  //        }
  //        //   val revisedVar=new Variable("Temperature_height_above_ground", SciData.apply("Temperature_height_above_ground").dataType, new Nd4jTensor(Data, shape), SciData.apply("Temperature_height_above_ground").attributes, dims)
  //        //        val revisedVar=new Variable(dataName, "short", new Nd4jTensor(Data, shape), scidata.apply(dataName).attributes, dims)
  //        val revisedVar=new Variable(dataName, DataV.dataType, new Nd4jTensor(Data, shape), scidata.apply(dataName).attributes, dims)
  //        revisedVar
  //      }
  //    )
  //
  //    suvals.foreach(suval=> (vals += suval) )
  //
  //    /*val varHash = SciData.variables.map(
  //      p => {
  //        p._1 match {
  //          case i if i.equals(dataName) => (p._1, revisedVar)
  //          case _ => p
  //        }
  //      })*/
  //    val sciDatasetName = scidata.datasetName
  //    val varHash=VariableOperation.allVars2NameVar4(this.getClass.toString,idName,vals,(timeName,heightName,latName,lonName))
  //
  //    val ocfdateStr=DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)
  //    var att = scidata.attributes
  //    att= AttributesOperation.addTrace(att,"RevisedByCity0["+ocfdateStr+","+timeStep+"]")
  //    new SciDataset(varHash, att, sciDatasetName)
  //  }
  //  /**
  //    * 定义指定区域内格点和ocf站号的匹配规则
  //    * @param beginLat
  //    * @param endLat
  //    * @param beginLon
  //    * @param endLon
  //    * @param spaceStep
  //    * @return
  //    */
  //  private def StationArr0(sciGeo: SciDataset, beginLat: Double, endLat: Double, beginLon: Double, endLon: Double, spaceStep: Double): Array[Double]={
  //    // 读取行政参数文件中的变量信息
  //    val distance = sciGeo.apply("distance").data()
  //    val forecast_id = sciGeo.apply("forecast_id").data()
  //    val level = sciGeo.apply("level").data()
  //    val latDims = sciGeo.apply("lat").shape()(1)
  //    val lonDims = sciGeo.apply("lon").shape()(1)
  //    val lat0 = sciGeo.apply("lat").data().head
  //    val lon0 = sciGeo.apply("lon").data().head
  //
  //    val splitlatDims = ((endLat-beginLat)/spaceStep +1).intValue()
  //    val splitlonDims = ((endLon-beginLon)/spaceStep +1).intValue()
  //    val splitlength =splitlatDims * splitlonDims
  //
  //    val flag = new Array[Double](splitlength)
  //    println("====行政区域面积===="+splitlength)
  //    for (j <- 0 until splitlength) {
  //      var P: Double = -999 //ocf订正标识符，初始值表示站点不覆盖格点
  //      val lat = (beginLat + (j/splitlonDims)*spaceStep)
  //      val lon = (beginLon + (j%splitlonDims)*spaceStep)
  //      val i = ((lat-lat0)/spaceStep * lonDims + (lon-lon0)/spaceStep).intValue()
  //      level(i) match { //判断ocf站号等级
  //        case ll if ll == 1 => P = forecast_id(i) //如果是省会城市直接覆盖
  //        case _ => {
  //          //如果为非省会城市
  //          //覆盖县级站点周边3km范围内的格点
  //          /*if(distance(i)< 3) {
  //              P = forecast_id(i)
  //            } */
  //        }
  //      }
  //      flag(j) = P
  //
  //    } //区域循环
  //    println("====行政区域参数文件加载完毕===="+flag.size)
  //    /*val nnn=VariableOperation.getIndex3_tlatlon(0,396,234,401,601)
  //    println(flag(238230)+";nnn="+nnn)*/
  //    flag
  //
  //  }
  //
  //  //对天气现象做特殊天气现象的全漏处理配合WeatherRule里的v2
  //  private def RevisedByCity1(scidata: SciDataset, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDataset, ocfMap:Map[String, Map[String,Map[String,Double]]]): SciDataset ={
  //    val idName=AttributesOperation.getIDName(scidata)
  //    val dataNames=PropertiesUtil.getrElement_rename(fcType,timeStep)
  //
  //    val fcdate=AttributesOperation.getfcdate(scidata)
  //    val fcdateStr=DateFormatUtil.YYYYMMDDHHStr0(fcdate)
  //
  //    val dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMddHH")
  //    val dateFormat2:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  //    val cal:Calendar=Calendar.getInstance()
  //
  //    val vals=new ArrayBuffer[Variable]
  //
  //    //获取变量名
  //    val (timeName,heightName,latName,lonName)=PropertiesUtil.varNames
  //
  //    val timeV=scidata.apply(timeName)
  //    val heightV=scidata.apply(heightName)
  //    val latV=scidata.apply(latName)
  //    val lonV=scidata.apply(lonName)
  //
  //    vals+= timeV+= heightV+= latV+= lonV
  //
  //    //获取数据维度大小信息
  //    val timeDims = scidata.apply(timeName).shape()(1)
  //    val heightDims =scidata.apply(heightName).shape()(1)
  //    val latDims = scidata.apply(latName).shape()(1)
  //    val lonDims = scidata.apply(lonName).shape()(1)
  //    val length = timeDims * heightDims * latDims * lonDims
  //    val planeLength = latDims * lonDims * heightDims
  //    val dims = List((timeName, timeDims), (heightName, heightDims), (latName, latDims), (lonName, lonDims))
  //    val shape = Array(timeDims, heightDims, latDims, lonDims)
  //    val beginTim = scidata.apply(timeName).data().head
  //    val endTim = scidata.apply(timeName).data().last
  //    //      val timeStep = (endTim - beginTim) / (timeDims - 1)
  //    val beginLat = scidata.apply(latName).data().head
  //    val endLat = scidata.apply(latName).data().last
  //    val beginLon = scidata.apply(lonName).data().head
  //    val endLon = scidata.apply(lonName).data().last
  //    val spaceStep = (endLon - beginLon) / (lonDims - 1)
  //    println(" latBegein + "+beginLat+" latEnd + "+endLat+" lonBegein + "+beginLon+" lonEnd +"+ endLon)
  //    //标记拆分区域内每个格点对应的ocf预报站号，没有对应关系的用代码-999表示，注意该数组是平面二维
  //    val flagArray = this.StationArr1(sciGeo,fcType, beginLat, endLat, beginLon, endLon, spaceStep)
  //
  //    val suvals=dataNames.map(
  //      dataName=>{
  //
  //        val ele:String={
  //          if(dataName.endsWith("Max")){
  //            fcType+"_Max"
  //          }else if(dataName.endsWith("Min")){
  //            fcType+"_Min"
  //          }else {
  //            fcType
  //          }
  //        }
  //
  //        //获取NMC要素预报数据
  //        val DataV=scidata.apply(dataName)
  //        val Data = DataV.data()
  //        //获取拆分区域的边界点和时空分辨率
  //
  //        //val flagArray = this.ReadGeoData(lat0, lon0, latDims, lonDims, level, distance, forecast_id,beginLat,endLat, beginLon, endLon, spaceStep)
  //        //  println("++++行政区域大小计算完毕:++++ "+    flagArray.count())
  //
  //        for(i <- 0 until planeLength) {
  //          val citySta = flagArray(i%(latDims * lonDims * heightDims)).toInt
  //          if(citySta != -999) { //如果地理和行政区域关系上有和县级站点的对应关系
  //            if(ocfMap.contains(citySta.toString)){ //如果ocf文件中存在该县级站站号
  //              for(t <- 0 until timeDims){
  //                val tstep =(timeStep * (t+1)).toInt
  //                if(tstep<=168){
  //                  cal.setTime(dateFormat.parse(fcdateStr))
  //                  cal.add(Calendar.HOUR_OF_DAY,tstep) //计算每个格点的nmc数据对应的预报时间
  //                  val time: String =dateFormat.format(cal.getTime)
  //                  if(ocfMap.get(citySta.toString).contains(time)){ //如果ocf文件中存在和nmc匹配的预报时间
  //                    if(ocfMap.get(citySta.toString).get.get(time).contains(ele)) { //如果ocf文件中存在nmc订正需要的要素
  //                      Data(i + t * planeLength) = ocfMap.get(citySta.toString).get(time).get(ele).get //则用ocf文件的数据覆盖nmc的格点数据
  //                      val ocfvalue=ocfMap.get(citySta.toString).get(time).get(ele).get
  //                      Data(i + t * planeLength)={
  //                        if(fcType.equals(ConstantUtil.WEATHER)){
  //                          val fcvalue=Data(i + t * planeLength)
  //                          PheRule.v2(fcvalue,ocfvalue)
  //                        }else{
  //                          ocfvalue
  //                        }
  //                      }
  //                    }
  //                  }
  //                }
  //              }
  //            }
  //          }
  //        }
  //        //   val revisedVar=new Variable("Temperature_height_above_ground", SciData.apply("Temperature_height_above_ground").dataType, new Nd4jTensor(Data, shape), SciData.apply("Temperature_height_above_ground").attributes, dims)
  //        //        val revisedVar=new Variable(dataName, "short", new Nd4jTensor(Data, shape), scidata.apply(dataName).attributes, dims)
  //        val revisedVar=new Variable(dataName, DataV.dataType, new Nd4jTensor(Data, shape), scidata.apply(dataName).attributes, dims)
  //        revisedVar
  //      }
  //    )
  //    suvals.foreach(suval=> (vals += suval) )
  //
  //    val sciDatasetName = scidata.datasetName
  //    val varHash=VariableOperation.allVars2NameVar4(this.getClass.toString,idName,vals,(timeName,heightName,latName,lonName))
  //
  //    val ocfdateStr=DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)
  //    val att = scidata.attributes
  //    val attnew= AttributesOperation.addTrace(att,"RevisedByCity1["+ocfdateStr+","+timeStep+"]")
  //    new SciDataset(varHash, attnew, sciDatasetName)
  //  }
  //  private def StationArr1(sciGeo: SciDataset,fcType:String, beginLat: Double, endLat: Double, beginLon: Double, endLon: Double, spaceStep: Double): Array[Double] ={
  //    // 读取行政参数文件中的变量信息
  //    val distance = sciGeo.apply("distance").data()
  //    val forecast_id = sciGeo.apply("forecast_id").data()
  //    val level = sciGeo.apply("level").data()
  //    val latDims = sciGeo.apply("lat").shape()(1)
  //    val lonDims = sciGeo.apply("lon").shape()(1)
  //    val lat0 = sciGeo.apply("lat").data().head
  //    val lon0 = sciGeo.apply("lon").data().head
  //
  //    val splitlatDims = ((endLat-beginLat)/spaceStep +1).intValue()
  //    val splitlonDims = ((endLon-beginLon)/spaceStep +1).intValue()
  //    val splitlength =splitlatDims * splitlonDims
  //
  //    val flag = new Array[Double](splitlength)
  //    println("====行政区域面积===="+splitlength)
  //
  //    //
  //    if(fcType.equals(ConstantUtil.WEATHER)){
  //      for (j <- 0 until splitlength) {
  //        var P: Double = -999 //ocf订正标识符，初始值表示站点不覆盖格点
  //        val lat = (beginLat + (j/splitlonDims)*spaceStep)
  //        val lon = (beginLon + (j%splitlonDims)*spaceStep)
  //        val i = ((lat-lat0)/spaceStep * lonDims + (lon-lon0)/spaceStep).intValue()
  //        flag(j) = level(i)
  //      }
  //    }else{
  //      for (j <- 0 until splitlength) {
  //        var P: Double = -999 //ocf订正标识符，初始值表示站点不覆盖格点
  //        val lat = (beginLat + (j/splitlonDims)*spaceStep)
  //        val lon = (beginLon + (j%splitlonDims)*spaceStep)
  //        val i = ((lat-lat0)/spaceStep * lonDims + (lon-lon0)/spaceStep).intValue()
  //        level(i) match { //判断ocf站号等级
  //          case ll if ll == 1 => P = forecast_id(i) //如果是省会城市直接覆盖
  //          case _ => {
  //            //如果为非省会城市
  //            //覆盖县级站点周边3km范围内的格点
  //            /*if(distance(i)< 3) {
  //                P = forecast_id(i)
  //              } */
  //          }
  //        }
  //        flag(j) = P
  //      } //区域循环
  //    }
  //
  //    println("====行政区域参数文件加载完毕===="+flag.size)
  //    /*val nnn=VariableOperation.getIndex3_tlatlon(0,396,234,401,601)
  //    println(flag(238230)+";nnn="+nnn)*/
  //    flag
  //  }


  //对v1做格式调整及算法标准化
  //  private def RevisedByCity2(scidata: SciDatasetCW, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap:Map[String, Map[String,Map[String,Double]]]): SciDatasetCW ={
  //    val idName=AttributesOperationCW.getIDName(scidata)
  //    val dataNames=PropertiesUtil.getrElement_rename(fcType,timeStep)
  //
  //    val ocfdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)
  //
  //    val fcdate=AttributesOperationCW.getfcdate(scidata)
  //    /*val scidata_timeStep=AttributesOperation.gettimeStep(scidata)
  //    val fcstartdate=WeatherDate.FcGetStartDate(fcdate,scidata_timeStep)*/
  //
  //    val ocfstartdate=WeatherDate.OcfGetStartDate(ocfdate)
  //    val ocfstartdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfstartdate)
  //
  //    val vals=new ArrayBuffer[VariableCW]
  //
  //    //获取变量名
  //    val (timeName, heightName, latName, lonName) = PropertiesUtil.varNames
  //
  //    val timeV=scidata.apply(timeName)
  //    val heightV=scidata.apply(heightName)
  //    val latV=scidata.apply(latName)
  //    val lonV=scidata.apply(lonName)
  //
  //    vals+= timeV+= heightV+= latV+= lonV
  //
  //    //获取数据维度信息
  //    val time=timeV.dataFloat()
  //    //    val height=heightV.dataFloat()
  //    //    val lat=latV.dataFloat()
  //    //    val lon=lonV.dataFloat()
  //
  //    val timeLen=timeV.getLen
  //    val heightLen=heightV.getLen
  //    val latLen=latV.getLen
  //    val lonLen=lonV.getLen
  //
  //    //
  //    val StationArray :Array[Array[String]]= this.StationArr2(scidata, sciGeo, fcType)
  //
  //    val suvals=dataNames.map(
  //      dataName=>{
  //
  //        val ele:String={
  //          if(dataName.endsWith("Max")){
  //            fcType+"_Max"
  //          }else if(dataName.endsWith("Min")){
  //            fcType+"_Min"
  //          }else {
  //            fcType
  //          }
  //        }
  //
  //        val DataV=scidata.apply(dataName)
  ////        val Data = DataV.data()
  //
  //
  //        for(k<- 0 to latLen-1){
  //          for(l<- 0 to lonLen-1){
  //            val stationId:String=StationArray(k)(l)
  //            if(!stationId.equals(this.defaultStation)){
  //              if (ocfMap.contains(stationId)) {
  //                val ocfMapStationId: Map[String, Map[String, Double]] = ocfMap.get(stationId).get
  //                for (i <- 0 to timeLen - 1) {
  //                  val time0: Float = time(i)
  //                  if (time0 <= timeLimit) {
  //                    /*val theDate_ocf=DateOperation.plusHour(ocfstartdate,time0.toInt)
  //                    val theDate_ocfStr=DataFormatUtil.YYYYMMDDHHStr0(theDate_ocf)*/
  //
  //                    val theDate_fc = DateOperation.plusHour(fcdate, time0.toInt)
  //                    val theDate_fcStr = DateFormatUtil.YYYYMMDDHHStr0(theDate_fc)
  //
  //                    if (ocfMapStationId.contains(theDate_fcStr)) {
  //                      val ocfvalue: Double = ocfMapStationId.get(theDate_fcStr).get(ele)
  //                      for (j <- 0 to heightLen - 1) {
  //                        val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
  //                        val fcvalue: Float = DataV.dataFloat(index)
  //                        DataV.setdata(index,OcfRule.ocf2(fcType, fcvalue, ocfvalue))
  //                      }
  //                    }
  //                  }
  //                }
  //              } else {
  //                val e: Exception = new Exception("idName=" + idName + ",ocf缺失站点:" + stationId + ",跳过运算")
  //                e.printStackTrace( )
  //              }
  //            }
  //          }
  //        }
  //
  //        /*for(k<- 0 to latLen-1){
  //          for(l<- 0 to lonLen-1){
  //            val stationId:String=StationArray(k)(l)
  //              if(!stationId.equals(this.defaultStation)){
  //                for(i<- 0 to timeLen-1){
  //                  val time0:Double=time(i)
  //                  if(time0<=timeLimit){
  //                    val theDate_ocf=DateOperation.plusHour(ocfstartdate,time0.toInt)
  //                    val theDate_ocfStr=DataFormatUtil.YYYYMMDDHHStr0(theDate_ocf)
  //                    for(j<- 0 to heightLen-1){
  //                      val ocfvalue:Double=ocfMap.get(stationId).get(theDate_ocfStr).get(ele).get
  //                      /*val ocfvalue:Double={
  //                        try{
  //                          ocfMap.get(stationId).get(theDateStr).get(ele).get
  //                        } catch{
  //                          case e:Exception=>{
  //                            val msg="stationId="+stationId+";theDateStr="+theDateStr+";time0="+time0+";ele="+ele+";ocfdate="+ocfdateStr+";timeStep="+timeStep+";fcdate="+fcdateStr
  //                            val e0=new Exception(e.toString+";"+msg)
  //                            //throw e0
  //                            e0.printStackTrace()
  //                            this.defaultV
  //                          }
  //                        }
  //                      }*/
  //                      val index=VariableOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)
  //                      val fcvalue:Double=Data(index)
  //                      Data(index)=OcfRule.ocf2(fcType,fcvalue,ocfvalue)
  //                    }
  //                  }
  //                }
  //              }
  //            }
  //          }*/
  //
  //
  //          DataV
  //  //        VariableOperation.creatVars4(Data,dataName,DataV.dataType,timeName,heightName,latName,lonName,timeLen,heightLen,latLen,lonLen)
  //        }
  //    )
  //    suvals.foreach(suval=> (vals += suval) )
  //
  //    val sciDatasetName = scidata.datasetName
  //
  //
  //    val att = scidata.attributes
  //    val attnew= AttributesOperation.addTrace(att,"RevisedByCity2[ocfdate="+ocfdateStr+"]")
  //    new SciDatasetCW(vals, attnew, sciDatasetName)
  //  }
  //  private def StationArr2(scidata:SciDatasetCW,sciGeo:SciDatasetCW,fcType:String): Array[Array[String]] ={
  //    val start=System.currentTimeMillis()
  //
  //    // 读取行政参数文件中的变量信息
  //    val geo_distanceV:VariableCW=sciGeo.apply("distance")
  //    val geo_forecast_idV:VariableCW=sciGeo.apply("forecast_id")
  //    val geo_levelV:VariableCW=sciGeo.apply("level")
  //    val geo_latV:VariableCW=sciGeo.apply("lat")
  //    val geo_lonV:VariableCW=sciGeo.apply("lon")
  //
  //
  //
  //    //    val geo_distance :Array[Float]= geo_distanceV.dataFloat()
  //    //    val geo_forecast_id :Array[Float]=geo_forecast_idV .dataFloat()
  //    //    val geo_level :Array[Int]=geo_levelV.dataInt()
  //
  //    val geo_lat = geo_latV.dataFloat()
  //    val geo_lon = geo_lonV.dataFloat()
  //
  //    val geo_latLen=geo_latV.getLen()
  //    val geo_lonLen=geo_lonV.getLen()
  //
  //    println("====行政区域面积====geo_latLen="+geo_latLen+",geo_lonLen="+geo_lonLen)
  //
  //
  //    //    val (timeName,heightName,latName,lonName)=PropertiesUtil.varNames
  //    val latV:VariableCW=scidata.apply(PropertiesUtil.latName)
  //    val lonV:VariableCW=scidata.apply(PropertiesUtil.lonName)
  //    val lat: Array[Float] =latV .dataFloat( )
  //    val lon: Array[Float] =lonV.dataFloat( )
  //    val latLen=latV.length
  //    val lonLen=lonV.length
  //
  //
  //    val stationArr :Array[Array[String]]= {
  //      val stationArr0:Array[Array[String]]=Array.ofDim[String](latLen,lonLen)
  //
  //      for(i<- 0 to latLen-1){
  //        val geoLatIndex=ArrayOperation.nearIndex_sequence(geo_lat,lat(i))
  //        for(j<- 0 to lonLen-1){
  //          val geoLonIndex=ArrayOperation.nearIndex_sequence(geo_lon,lon(j))
  //
  //          val geoIndex = IndexOperation.getIndex2_latlon(geoLatIndex, geoLonIndex, geo_lonLen)
  //          stationArr0(i)(j)={
  //            if (fcType.equals(Constant.WEATHER)) {
  //              geo_forecast_idV.dataInt(geoIndex).toString
  //            }else{
  //              if(geo_levelV.dataInt(geoIndex)==1){
  //                geo_forecast_idV.dataInt(geoIndex).toString
  //              }else{
  //                //做不订正处理
  //                this.defaultStation
  //              }
  //            }
  //          }
  //        }
  //      }
  //
  //      stationArr0
  //    }
  //
  //
  //
  //
  //    //    println("====行政区域参数文件加载完毕====")
  //    /*val nnn=VariableOperation.getIndex3_tlatlon(0,396,234,401,601)
  //    println(flag(238230)+";nnn="+nnn)*/
  //
  //    val end=System.currentTimeMillis()
  //    WeatherShowtime.showDateStrOut1("行政区域参数文件加载",start,end,"geo_latLen="+geo_latLen+";geo_lonLen="+geo_lonLen)
  //
  //    stationArr
  //  }

  //消除StationArr
  private def RevisedByCity3(scidata: SciDatasetCW, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap:Map[String, Map[String, Map[String, Float]]]): SciDatasetCW ={
    val idName=AttributesOperationCW.getIDName(scidata)
    val dataNames=PropertiesUtil.getrElement_rename(fcType,timeStep)

    val ocfdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)

    val fcdate=AttributesOperationCW.getfcdate(scidata)
    /*val scidata_timeStep=AttributesOperation.gettimeStep(scidata)
    val fcstartdate=WeatherDate.FcGetStartDate(fcdate,scidata_timeStep)*/

    val ocfstartdate=WeatherDate.OcfGetStartDate(ocfdate)
    val ocfstartdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfstartdate)

    val vals=new ArrayBuffer[VariableCW]

    //获取变量名
    val (timeName, heightName, latName, lonName) = PropertiesUtil.getVarName()

    val timeV=scidata.apply(timeName)
    val heightV=scidata.apply(heightName)
    val latV=scidata.apply(latName)
    val lonV=scidata.apply(lonName)

    vals+= timeV+= heightV+= latV+= lonV

    //获取数据维度信息
    //    val time=timeV.dataFloat()
    //    val height=heightV.dataFloat()
    //    val lat=latV.dataFloat()
    //    val lon=lonV.dataFloat()

    val timeLen=timeV.getLen
    val heightLen=heightV.getLen
    val latLen=latV.getLen
    val lonLen=lonV.getLen

    //

    val geo_distanceV:VariableCW=sciGeo.apply("distance")
    val geo_forecast_idV:VariableCW=sciGeo.apply("forecast_id")
    val geo_levelV:VariableCW=sciGeo.apply("level")
    val geo_latV:VariableCW=sciGeo.apply("lat")
    val geo_lonV:VariableCW=sciGeo.apply("lon")

    val geo_latLen=geo_latV.getLen()
    val geo_lonLen=geo_lonV.getLen()
    val geo_lat = geo_latV.dataFloat()
    val geo_lon = geo_lonV.dataFloat()


    val suvals=dataNames.map(
      dataName=>{
        //        val dataName=dataName_ele._1
        val ele:String={
          if(dataName.endsWith("Max")){
            fcType+"_Max"
          }else if(dataName.endsWith("Min")){
            fcType+"_Min"
          }else {
            fcType
          }
        }
        val DataV=scidata.apply(dataName)



        for(k<- 0 to latLen-1){
          val geoLatIndex=ArrayOperation.nearIndex_sequence(geo_lat,latV.dataFloat(k))
          for(l<- 0 to lonLen-1){
            val geoLonIndex=ArrayOperation.nearIndex_sequence(geo_lon,lonV.dataFloat(l))
            val geoIndex = IndexOperation.getIndex2_latlon(geoLatIndex, geoLonIndex, geo_lonLen)
            val stationId:String={
              if (fcType.equals(Constant.WEATHER)) {
                geo_forecast_idV.dataInt(geoIndex).toString
              }else{
                if(geo_levelV.dataInt(geoIndex)==1){
                  geo_forecast_idV.dataInt(geoIndex).toString
                }else{
                  //做不订正处理
//                  break
                  this.defaultStation
                }
              }
            }
            if(!stationId.equals(this.defaultStation)){
              if(ocfMap.contains(stationId)){
                val ocfMapStation=ocfMap.get(stationId).get
                for(i<- 0 to timeLen-1){
                  val timeInt:Int=timeV.dataInt(i)
                  if(timeInt<=timeLimit){
                    val theDate_ocf:Date=DateOperation.plusHour(fcdate,timeInt)
                    val theDate_ocfStr=DateFormatUtil.YYYYMMDDHHStr0(theDate_ocf)
                    if(ocfMapStation.contains(theDate_ocfStr)){
                      val ocfvalue:Float=ocfMapStation.get(theDate_ocfStr).get(ele)
                      for(j<- 0 to heightLen-1){
                        val index=IndexOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)
                        val fcvalue:Float=DataV.dataFloat(index)
                        DataV.setdata(index,OcfRule.ocf2(fcType, fcvalue, ocfvalue))
                      }
//                    }else{
//                      val msg="idName=" + idName + ",ocf缺失站点:" + stationId +",fcType="+fcType+ ",ele="+ele+";timeStep="+timeStep+",theDateStr="+theDate_ocfStr+",lat="+latV.dataFloat(k)+",lon="+lonV.dataFloat(l)+",跳过运算"
//                      //                    val e: Exception = new Exception()
//                      //                    e.printStackTrace( )
//                      println(msg)
                    }
//                  }else{
//                    val msg="ocf超过预设订正天数:"
//                    val e:Exception=new Exception(msg)
//                    e.printStackTrace()
                  }
                }
              }
            }

          }
        }

        /*for(k<- 0 to latLen-1){
          for(l<- 0 to lonLen-1){
            val stationId:String=StationArray(k)(l)
            if(!stationId.equals(this.defaultStation)){
              for(i<- 0 to timeLen-1){
                val time0:Double=time(i)
                if(time0<=timeLimit){
                  val theDate_ocf=DateOperation.plusHour(ocfstartdate,time0.toInt)
                  val theDate_ocfStr=DataFormatUtil.YYYYMMDDHHStr0(theDate_ocf)
                  for(j<- 0 to heightLen-1){
                    val ocfvalue:Double=ocfMap.get(stationId).get(theDate_ocfStr).get(ele).get
                    /*val ocfvalue:Double={
                      try{
                        ocfMap.get(stationId).get(theDateStr).get(ele).get
                      } catch{
                        case e:Exception=>{
                          val msg="stationId="+stationId+";theDateStr="+theDateStr+";time0="+time0+";ele="+ele+";ocfdate="+ocfdateStr+";timeStep="+timeStep+";fcdate="+fcdateStr
                          val e0=new Exception(e.toString+";"+msg)
                          //throw e0
                          e0.printStackTrace()
                          this.defaultV
                        }
                      }
                    }*/
                    val index=VariableOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)
                    val fcvalue:Double=Data(index)
                    Data(index)=OcfRule.ocf2(fcType,fcvalue,ocfvalue)
                  }
                }
              }
            }
          }
        }*/


        DataV
        //        VariableOperation.creatVars4(Data,dataName,DataV.dataType,timeName,heightName,latName,lonName,timeLen,heightLen,latLen,lonLen)
      }
    )
    suvals.foreach(suval=> (vals += suval) )

    val sciDatasetName = scidata.datasetName


    val att = scidata.attributes
    val attnew= AttributesOperationCW.addTrace(att,"RevisedByCity2[ocfdate="+ocfdateStr+"]")
    new SciDatasetCW(vals, attnew, sciDatasetName)
  }


  //消除StationArr
  //  private def RevisedByCity4(scidata: SciDatasetCW, fcType:String, ocfdate: Date, timeStep:Double, sciGeo:SciDatasetCW, ocfMap:mutable.HashMap[(String, String, String), Float]): SciDatasetCW ={
  //    val idName=AttributesOperationCW.getIDName(scidata)
  //    val dataNames=PropertiesUtil.getrElement_rename(fcType,timeStep)
  //
  //    val ocfdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfdate)
  //
  //    val fcdate=AttributesOperationCW.getfcdate(scidata)
  //    /*val scidata_timeStep=AttributesOperation.gettimeStep(scidata)
  //    val fcstartdate=WeatherDate.FcGetStartDate(fcdate,scidata_timeStep)*/
  //
  //    val ocfstartdate=WeatherDate.OcfGetStartDate(ocfdate)
  //    val ocfstartdateStr = DateFormatUtil.YYYYMMDDHHMMSSStr0(ocfstartdate)
  //
  //    val vals=new ArrayBuffer[VariableCW]
  //
  //    //获取变量名
  //    val (timeName, heightName, latName, lonName) = PropertiesUtil.varNames
  //
  //    val timeV=scidata.apply(timeName)
  //    val heightV=scidata.apply(heightName)
  //    val latV=scidata.apply(latName)
  //    val lonV=scidata.apply(lonName)
  //
  //    vals+= timeV+= heightV+= latV+= lonV
  //
  //    //获取数据维度信息
  //    //    val time=timeV.dataFloat()
  //    //    val height=heightV.dataFloat()
  //    //    val lat=latV.dataFloat()
  //    //    val lon=lonV.dataFloat()
  //
  //    val timeLen=timeV.getLen
  //    val heightLen=heightV.getLen
  //    val latLen=latV.getLen
  //    val lonLen=lonV.getLen
  //
  //    //
  //
  //    val geo_distanceV:VariableCW=sciGeo.apply("distance")
  //    val geo_forecast_idV:VariableCW=sciGeo.apply("forecast_id")
  //    val geo_levelV:VariableCW=sciGeo.apply("level")
  //    val geo_latV:VariableCW=sciGeo.apply("lat")
  //    val geo_lonV:VariableCW=sciGeo.apply("lon")
  //
  //    val geo_latLen=geo_latV.getLen()
  //    val geo_lonLen=geo_lonV.getLen()
  //    val geo_lat = geo_latV.dataFloat()
  //    val geo_lon = geo_lonV.dataFloat()
  //
  //    //    val StationArray :Array[Array[String]]= this.StationArr3(scidata, sciGeo, fcType)
  //
  //    val dataName_eles:Array[(String,String)]=dataNames.map(dataName=>{
  //      val ele:String={
  //        if(dataName.endsWith("Max")){
  //          fcType+"_Max"
  //        }else if(dataName.endsWith("Min")){
  //          fcType+"_Min"
  //        }else {
  //          fcType
  //        }
  //      }
  //      (dataName,ele)
  //    })
  //    val eles:Array[String]=dataName_eles.map(dataName_ele=>dataName_ele._2)
  //
  //    ocfMap.foreach(p=>{
  //      if(!eles.contains(p._1._3)){
  //        ocfMap.remove(p._1)
  //      }
  //    })
  //
  //
  //    val suvals=dataName_eles.map(
  //      dataName_ele=>{
  //        val dataName=dataName_ele._1
  //        val ele=dataName_ele._2
  //
  //        val DataV=scidata.apply(dataName)
  //
  //
  //
  //        for(k<- 0 to latLen-1){
  //          val geoLatIndex=ArrayOperation.nearIndex_sequence(geo_lat,latV.dataFloat(k))
  //          for(l<- 0 to lonLen-1){
  //            val geoLonIndex=ArrayOperation.nearIndex_sequence(geo_lon,lonV.dataFloat(l))
  //            val geoIndex = IndexOperation.getIndex2_latlon(geoLatIndex, geoLonIndex, geo_lonLen)
  //            val stationId:String={
  //              if (fcType.equals(Constant.WEATHER)) {
  //                geo_forecast_idV.dataInt(geoIndex).toString
  //              }else{
  //                if(geo_levelV.dataInt(geoIndex)==1){
  //                  geo_forecast_idV.dataInt(geoIndex).toString
  //                }else{
  //                  //做不订正处理
  //                  break
  //                  //this.defaultStation
  //                }
  //              }
  //            }
  //
  //            if(!stationId.equals(this.defaultStation)){
  //              //              if (ocfMap.contains(stationId)) {
  //              //                val ocfMapStationId: Map[String, Map[String, Float]] = ocfMap.get(stationId).get
  //              for (i <- 0 to timeLen - 1) {
  //                val time0: Int = timeV.dataInt(i)
  //                if (time0 <= timeLimit) {
  //
  //                  val theDate_fc = DateOperation.plusHour(fcdate, time0)
  //                  val theDate_fcStr = DateFormatUtil.YYYYMMDDHHStr0(theDate_fc)
  //                  if(ocfMap.contains((stationId,theDate_fcStr,ele))){
  //                    for (j <- 0 to heightLen - 1) {
  //                      val index = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
  //                      val fcvalue: Float = DataV.dataFloat(index)
  //                      val ocfvalue: Float = ocfMap.remove((stationId,theDate_fcStr,ele)).get
  //                      DataV.setdata(index,OcfRule.ocf2(fcType, fcvalue, ocfvalue))
  //                    }
  //                  }/*else{
  //                    val msg="idName=" + idName + ",ocf缺失站点:" + stationId +",fcType="+fcType+ ",ele="+ele+";timeStep="+timeStep+",theDateStr="+theDate_fcStr+",lat="+latV.dataFloat(k)+",lon="+lonV.dataFloat(l)+",跳过运算"
  //                    val e: Exception = new Exception()
  //                    e.printStackTrace( )
  //                  }*/
  //                }
  //              }
  //              //              } else {
  //
  //              //              }
  //            }
  //          }
  //        }
  //
  //        /*for(k<- 0 to latLen-1){
  //          for(l<- 0 to lonLen-1){
  //            val stationId:String=StationArray(k)(l)
  //            if(!stationId.equals(this.defaultStation)){
  //              for(i<- 0 to timeLen-1){
  //                val time0:Double=time(i)
  //                if(time0<=timeLimit){
  //                  val theDate_ocf=DateOperation.plusHour(ocfstartdate,time0.toInt)
  //                  val theDate_ocfStr=DataFormatUtil.YYYYMMDDHHStr0(theDate_ocf)
  //                  for(j<- 0 to heightLen-1){
  //                    val ocfvalue:Double=ocfMap.get(stationId).get(theDate_ocfStr).get(ele).get
  //                    /*val ocfvalue:Double={
  //                      try{
  //                        ocfMap.get(stationId).get(theDateStr).get(ele).get
  //                      } catch{
  //                        case e:Exception=>{
  //                          val msg="stationId="+stationId+";theDateStr="+theDateStr+";time0="+time0+";ele="+ele+";ocfdate="+ocfdateStr+";timeStep="+timeStep+";fcdate="+fcdateStr
  //                          val e0=new Exception(e.toString+";"+msg)
  //                          //throw e0
  //                          e0.printStackTrace()
  //                          this.defaultV
  //                        }
  //                      }
  //                    }*/
  //                    val index=VariableOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)
  //                    val fcvalue:Double=Data(index)
  //                    Data(index)=OcfRule.ocf2(fcType,fcvalue,ocfvalue)
  //                  }
  //                }
  //              }
  //            }
  //          }
  //        }*/
  //
  //
  //        DataV
  //        //        VariableOperation.creatVars4(Data,dataName,DataV.dataType,timeName,heightName,latName,lonName,timeLen,heightLen,latLen,lonLen)
  //      }
  //    )
  //    suvals.foreach(suval=> (vals += suval) )
  //
  //    val sciDatasetName = scidata.datasetName
  //
  //
  //    val att = scidata.attributes
  //    val attnew= AttributesOperation.addTrace(att,"RevisedByCity2[ocfdate="+ocfdateStr+"]")
  //    new SciDatasetCW(vals, attnew, sciDatasetName)
  //  }
  //  private def StationArr3(scidata:SciDatasetCW,sciGeo:SciDatasetCW,fcType:String): Array[Array[String]] ={
  //    val start=System.currentTimeMillis()
  //
  //    // 读取行政参数文件中的变量信息
  //    val geo_distanceV:VariableCW=sciGeo.apply("distance")
  //    val geo_forecast_idV:VariableCW=sciGeo.apply("forecast_id")
  //    val geo_levelV:VariableCW=sciGeo.apply("level")
  //    val geo_latV:VariableCW=sciGeo.apply("lat")
  //    val geo_lonV:VariableCW=sciGeo.apply("lon")
  //
  //
  //
  //    //    val geo_distance :Array[Float]= geo_distanceV.dataFloat()
  //    //    val geo_forecast_id :Array[Float]=geo_forecast_idV .dataFloat()
  //    //    val geo_level :Array[Int]=geo_levelV.dataInt()
  //
  //    val geo_lat = geo_latV.dataFloat()
  //    val geo_lon = geo_lonV.dataFloat()
  //
  //    val geo_latLen=geo_latV.getLen()
  //    val geo_lonLen=geo_lonV.getLen()
  //
  //    println("====行政区域面积====geo_latLen="+geo_latLen+",geo_lonLen="+geo_lonLen)
  //
  //
  //    //    val (timeName,heightName,latName,lonName)=PropertiesUtil.varNames
  //    val latV:VariableCW=scidata.apply(PropertiesUtil.latName)
  //    val lonV:VariableCW=scidata.apply(PropertiesUtil.lonName)
  //    val lat: Array[Float] =latV .dataFloat( )
  //    val lon: Array[Float] =lonV.dataFloat( )
  //    val latLen=latV.length
  //    val lonLen=lonV.length
  //
  //
  //    val stationArr :Array[Array[String]]= {
  //      val stationArr0:Array[Array[String]]=Array.ofDim[String](latLen,lonLen)
  //
  //      for(i<- 0 to latLen-1){
  //        val geoLatIndex=ArrayOperation.nearIndex_sequence(geo_lat,lat(i))
  //        for(j<- 0 to lonLen-1){
  //          val geoLonIndex=ArrayOperation.nearIndex_sequence(geo_lon,lon(j))
  //
  //          val geoIndex = IndexOperation.getIndex2_latlon(geoLatIndex, geoLonIndex, geo_lonLen)
  //          stationArr0(i)(j)={
  //            if (fcType.equals(Constant.WEATHER)) {
  //              geo_forecast_idV.dataInt(geoIndex).toString
  //            }else{
  //              if(geo_levelV.dataInt(geoIndex)==1){
  //                geo_forecast_idV.dataInt(geoIndex).toString
  //              }else{
  //                //做不订正处理
  //                this.defaultStation
  //              }
  //            }
  //          }
  //        }
  //      }
  //
  //      stationArr0
  //    }
  //
  //
  //
  //
  //    //    println("====行政区域参数文件加载完毕====")
  //    /*val nnn=VariableOperation.getIndex3_tlatlon(0,396,234,401,601)
  //    println(flag(238230)+";nnn="+nnn)*/
  //
  //    val end=System.currentTimeMillis()
  //    WeatherShowtime.showDateStrOut1("行政区域参数文件加载",start,end,"geo_latLen="+geo_latLen+";geo_lonLen="+geo_lonLen)
  //
  //    stationArr
  //  }

}