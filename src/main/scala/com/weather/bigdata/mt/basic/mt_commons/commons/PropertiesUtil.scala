package com.weather.bigdata.mt.basic.mt_commons.commons

import java.io.{File, InputStream}
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.SplitMatchInfo
import org.apache.log4j.Logger

import scala.collection.mutable

object PropertiesUtil {
  val log:Logger=Logger.getRootLogger
  private val FcTypes = Constant.OrdinaryFcTypes
  private val prop:java.util.Properties={
    val prop = new java.util.Properties()
    val loader = Thread.currentThread.getContextClassLoader()
    val loadfile: InputStream=loader.getResourceAsStream("mt_commons.properties")
    prop.load(loadfile)
    prop
  }
  /**
    * 是否是生产环境
    */
  val isPrd:Boolean=this.prop.getProperty("is.product").toBoolean
  //  val isPrd:Boolean=(!ContextUtil.getSparkContext().isLocal)
  /**
    * 选择写nc的netCDF版本,pom.xml中有配置
    */
  val writeCDFVersion=this.prop.getProperty("writeCDFVersion")
  val netCDFDependency=this.prop.getProperty("netCDFDependency")

  val overwrite:Boolean=this.prop.getProperty("overwrite").toBoolean
  //  val reftimeOpem: Boolean = this.prop.getProperty("reftimeOpen").toBoolean
  /*val hdfsmasterName:String={
    if(this.isPrd){
      "hdfs://"+this.prop.getProperty("masterName")
    }else{
      "file://"
    }
  }*/
  val masterName=this.prop.getProperty("masterName")
  val head:String={
    if(this.isPrd){
      "hdfs:/"
    }else{
      "file:/"
    }
  }
  val hdfsmasterName:String={
    head+"/"+masterName
  }

  val VarNameMap:java.util.HashMap[String,String]=new java.util.HashMap[String,String]()
  def getVarName():(String,String,String,String)={
    /*val timeName=this.prop.getProperty("timeName")
    val heightName=this.prop.getProperty("heightName")
    val latName=this.prop.getProperty("latName")
    val lonName=this.prop.getProperty("lonName")*/
    var timeName=this.VarNameMap.get("timeName")
    var heightName=this.VarNameMap.get("heightName")
    var latName=this.VarNameMap.get("latName")
    var lonName=this.VarNameMap.get("lonName")
    val msg0="timeName="+timeName+";heightName="+heightName+";latName="+latName+";lonName="+lonName
    if("".equals(timeName)|| "".equals(heightName)|| "".equals(latName)||"".equals(lonName)) {
      /*val e:Exception=new Exception("timeName="+timeName+";heightName="+heightName+";latName="+latName+";lonName="+lonName)
      e.printStackTrace()*/
      if ("".equals(timeName)) {
        timeName = this.prop.getProperty("timeName")
      }
      if ("".equals(heightName)) {
        heightName = this.prop.getProperty("heightName")
      }
      if ("".equals(latName)) {
        latName = this.prop.getProperty("latName")
      }
      if ("".equals(lonName)) {
        lonName = this.prop.getProperty("lonName")
      }
      val msg = "timeName=" + timeName + ";heightName=" + heightName + ";latName=" + latName + ";lonName=" + lonName
      println("提取time,height,lat,lon参数：" + msg0 + ";已传入json信息,未定义的使用pom.xml中定义的参数—>" + msg)
    }else if(timeName==null || heightName==null || latName==null || lonName==null){
      if (timeName ==null ) {
        timeName = this.prop.getProperty("timeName")
      }
      if (heightName ==null ) {
        heightName = this.prop.getProperty("heightName")
      }
      if (latName ==null ) {
        latName = this.prop.getProperty("latName")
      }
      if (lonName ==null) {
        lonName = this.prop.getProperty("lonName")
      }
      val msg = "timeName=" + timeName + ";heightName=" + heightName + ";latName=" + latName + ";lonName=" + lonName
      println("提取time,height,lat,lon参数：" + msg0 + ";未传入json信息,使用pom.xml中定义的参数—>" + msg)
    }else{
      println("提取time,height,lat,lon参数：" +msg0)
    }
    (timeName,heightName,latName,lonName)
  }

  val mainJar=this.prop.getProperty("mainJar")

  val inputKVOpen:Boolean=this.prop.getProperty("inputKVOpen").toBoolean
  val inputKVStoreOutOpen:Boolean= this.prop.getProperty("inputKVStoreOutOpen").toBoolean
  val gr2bForwardOpen:Boolean=this.prop.getProperty("gr2bForwardOpen").toBoolean
  val changeRFCover:Boolean=this.prop.getProperty("changeRFCover").toBoolean
  val changeRFAllCover: Boolean = this.prop.getProperty("changeRFCoverAll").toBoolean
  val binEnvpath:String=this.prop.getProperty("binEnvpath")
  val indexsignalOpen:Boolean=this.prop.getProperty("indexsignalOpen").toBoolean
  val obssignalOpen:Boolean=this.prop.getProperty("obssignalOpen").toBoolean
  val reduceLatLonsignalOpen: Boolean = this.prop.getProperty("reduceLatLonsignalOpen").toBoolean
  val changeReftimeOpen:Boolean=this.prop.getProperty("changeReftimeOpen").toBoolean

  //    val weatherName:String=this.prop.getProperty("weatherName")
  val weatherName: String = Constant.WEATHER



  val (
    latName_Obs:String,
    lonName_Obs:String,
    pre_Obs:String,
    tem_Obs: String
    ) = {
    (
      this.prop.getProperty("latName_Obs"),
      this.prop.getProperty("lonName_Obs"),
      this.prop.getProperty("pre_Obs"),
      this.prop.getProperty("tem_Obs")
    )
  }

  val (
    timeName_pph: String,
    latName_pph: String,
    lonName_pph: String,
    dataName_pph: String
    ) = {
    (
      this.prop.getProperty("timeName_PPH"),
      this.prop.getProperty("latName_PPH"),
      this.prop.getProperty("lonName_PPH"),
      this.prop.getProperty("dataName_PPH")
    )
  }

  def getVarName_pph (): (String, String, String) = {
    (timeName_pph, latName_pph, lonName_pph)
  }

  //  val (
  //    fcParallel: Boolean,
  //    obsParallel: Boolean,
  //    ocfParallel: Boolean,
  //    wea0Parallel: Boolean
  //    ) = {
  //    (
  //      this.prop.getProperty("fcParallelOpen").toBoolean,
  //      this.prop.getProperty("obsParallelOpen").toBoolean,
  //      this.prop.getProperty("ocfParallelOpen").toBoolean,
  //      this.prop.getProperty("wea0ParallelOpen").toBoolean
  //    )
  //  }

  val (
    gr2bOpen:Boolean,
    gr2bFrontOpen:Boolean,
    fcPostOpen:Boolean,
    obsOpen:Boolean,
    ocfOpen:Boolean,
    weaOpen: Boolean,
    checkStationOpen:Boolean,
    checkFileOpen: Boolean
    )={(
    this.prop.getProperty("gr2bOpen").toBoolean,
    this.prop.getProperty("gr2bFrontOpen").toBoolean,
    this.prop.getProperty("fcPostOpen").toBoolean,
    this.prop.getProperty("obsOpen").toBoolean,
    this.prop.getProperty("ocfOpen").toBoolean,
    this.prop.getProperty("weaOpen").toBoolean,
    this.prop.getProperty("checkStationOpen").toBoolean,
    this.prop.getProperty("checkFileOpen").toBoolean
  )
  }

  val latlon: String = this.prop.getProperty("LatLon")

  def getObsDataName(FcType:String): String ={
    if (Constant.PRE.equals(FcType)) {
      this.pre_Obs
    } else if (Constant.TEM.equals(FcType)) {
      this.tem_Obs
    }else{
      null
    }
  }
  def getObsDataName(FcTypes:Array[String]): Array[String] ={
    FcTypes.map(fcType=>this.getObsDataName(fcType))
  }

  def getIndexSignalFile(fcdate:Date):String={
    val dateStr = DateFormatUtil.YYYYMMDDHHStr0(fcdate)
    //    val timeStamp=System.currentTimeMillis()
    //    this.filePath(this.IndexSignalRootPath,fcdate)+"/monitor.txt"
    this.indexSignalRootPath+"/monitor.txt"
  }
  def getObsSignalFile(fcdate:Date,fcType:String): String ={
    val dateStr = DateFormatUtil.YYYYMMDDHHStr0(fcdate)
    //    val now:Long=System.currentTimeMillis()
    if(this.obsSignalRootPath.endsWith("/")){
      this.obsSignalRootPath+fcType+"/"+dateStr+".txt"
    }else{
      this.obsSignalRootPath+"/"+fcType+"/"+dateStr+".txt"
    }
  }

  def getReduceLatLonSignalFile(fcdate: Date, fcType: String): String = {
    val dateStr = DateFormatUtil.YYYYMMDDHHStr0(fcdate)
    val timeStamp = System.currentTimeMillis( )
    val dateStr0 = DateFormatUtil.YYYYMMDDStr0(new Date(timeStamp))
    //    val fcTypeStr=fcTypes.reduce((x,y)=>(x+"_"+y))

    //    val now:Long=System.currentTimeMillis()
    val path = {
      if (this.reduceLatLonRootPath.endsWith("/")) {
        this.reduceLatLonRootPath + dateStr0 + "/"
      } else {
        this.reduceLatLonRootPath + "/" + dateStr0 + "/"
      }
    }
    if (!HDFSOperation1.exists(path)) {
      HDFSOperation1.mkdirs(path)
    }
    val fileName: String = "reduceLatLon_" + fcType + "_" + dateStr + "_" + timeStamp + ".txt"
    path + fileName
  }

  private val writeTmp_local:String= this.prop.getProperty("writePathTmp_local")



  //抽取站点的地址
  val (
    stationchangeReftime:String,
    stationfc:String,
    stationocf:String,
    stationobs:String,
    stationinputKV:String
    )={
    (
      this.prop.getProperty("stationchangeReftime"),
      this.prop.getProperty("stationfc"),
      this.prop.getProperty("stationocf"),
      this.prop.getProperty("stationobs"),
      this.prop.getProperty("stationinputKV")
    )
  }
  //输出nc的地址
  val (
    //    checkoutRootPath:String,
    checkoutRootPath2:String,
    stationfile0:String,
    stationfile1:String,
    stationfile_stainfo_nat:String,
    inputKVCheckoutRootPath:String
    )={
    if(this.isPrd){
      (
        //        hdfsmasterName+this.prop.getProperty("checkoutRootPath"),
        hdfsmasterName+this.prop.getProperty("checkoutRootPath2"),
        hdfsmasterName+this.prop.getProperty("stationfile0"),
        hdfsmasterName+this.prop.getProperty("stationfile1"),
        hdfsmasterName+this.prop.getProperty("stationfile_stainfo_nat"),
        hdfsmasterName+this.prop.getProperty("inputKVCheckoutRootPath")
      )
    }else{
      (
        //        this.prop.getProperty("checkoutRootPath"),
        this.prop.getProperty("checkoutRootPath2"),
        this.prop.getProperty("stationfile0"),
        this.prop.getProperty("stationfile1"),
        this.prop.getProperty("stationfile_stainfo_nat"),
        this.prop.getProperty("inputKVCheckoutRootPath")
      )
    }
  }

  //预警库参数
//  val (
//    alarmURL:String,
//    alarmName:String,
//    alarmTable:String,
//    alarmIDTable:String,
//    alarmUser:String,
//    alarmPassWord:String
//    )={
//    (
//      this.prop.getProperty("alarmURL"),
//      this.prop.getProperty("alarmName"),
//      this.prop.getProperty("alarmTable"),
//      this.prop.getProperty("alarmIDTable"),
//      this.prop.getProperty("alarmUser"),
//      this.prop.getProperty("alarmPassWord")
//    )
//  }

  private val (
    indexSignalRootPath:String,
    obsSignalRootPath:String,
    reduceLatLonRootPath: String,
    writeTmp_hdfs:String,
    jsonFile:String,
    //    jsonFileTest:String,
    jsonPath:String,
    fcPath:String,
    //    afterobs_fcPath:String,
    gr2bPath:String,
    obsRootPath:String,
    ocfRootPath:String,
    //    geoPath:String,
    geoFile:String,

    streamRootPath:String,
    streamSecPath:String,
    typeSetRootpath:String,
    temExtremPath:String,

    inputKVStorePath: String,
    reduceLatLonPath
    ) = {
    if(this.isPrd){
      (
        hdfsmasterName+this.prop.getProperty("indexsignalPath"),
        hdfsmasterName+this.prop.getProperty("obssignalPath"),
        hdfsmasterName + this.prop.getProperty("reduceLatLonRootPath"),
        hdfsmasterName+this.prop.getProperty("writePathTmp_hdfs"),
        hdfsmasterName+this.prop.getProperty("jsonFile"),
        //        hdfsmasterName+this.prop.getProperty("jsonFileTest"),
        hdfsmasterName+this.prop.getProperty("jsonPath"),
        hdfsmasterName+this.prop.getProperty("fcPath"),
        //        hdfsmasterName+this.prop.getProperty("afterObsfcPath"),
        hdfsmasterName+this.prop.getProperty("gr2bPath"),
        hdfsmasterName+this.prop.getProperty("obsRootPath"),
        hdfsmasterName+this.prop.getProperty("ocfRootPath"),
        //        hdfsmasterName+this.prop.getProperty("geoPath"),
        hdfsmasterName+this.prop.getProperty("geoFile"),

        hdfsmasterName+this.prop.getProperty("streamRootPath"),
        hdfsmasterName+this.prop.getProperty("streamSecPath"),
        hdfsmasterName+this.prop.getProperty("typeSetRootpath"),
        hdfsmasterName+this.prop.getProperty("temExtremPath"),

        hdfsmasterName + this.prop.getProperty("inputKVStorePath"),
        hdfsmasterName + this.prop.getProperty("reduceLatLonPath")
      )
    }else{
      (
        this.prop.getProperty("indexsignalPath"),
        this.prop.getProperty("obssignalPath"),
        this.prop.getProperty("reduceLatLonRootPath"),
        this.prop.getProperty("writePathTmp_hdfs"),
        this.prop.getProperty("jsonFile"),
        //        this.prop.getProperty("jsonFileTest"),
        this.prop.getProperty("jsonPath"),
        this.prop.getProperty("fcPath"),
        //        this.prop.getProperty("afterObsfcPath"),
        this.prop.getProperty("gr2bPath"),
        this.prop.getProperty("obsRootPath"),
        this.prop.getProperty("ocfRootPath"),
        //        this.prop.getProperty("geoPath"),
        this.prop.getProperty("geoFile"),

        this.prop.getProperty("streamRootPath"),
        this.prop.getProperty("streamSecPath"),
        this.prop.getProperty("typeSetRootpath"),
        this.prop.getProperty("temExtremPath"),


        this.prop.getProperty("inputKVStorePath"),
        this.prop.getProperty("reduceLatLonPath")
      )
    }
  }

  private val GPathHashMap_pom:java.util.HashMap[String,String]={
    val hashmap=new java.util.HashMap[String,String]()
    this.FcTypes.foreach(FcType=>hashmap.put(FcType,this.prop.getProperty(FcType+"_GribPath")))
    hashmap
  }
  private val GPathHashMap_json:java.util.HashMap[String,String]=new java.util.HashMap[String,String]()
  private val reNameHashMap_json:java.util.HashMap[String,String]=new util.HashMap[String,String]()
  private val rElementHashMap_json:java.util.HashMap[String,Array[String]]=new java.util.HashMap[String,Array[String]]()

  private def getWriteTmp_local():String={
//    FileOperation.pathCreat(this.writeTmp_local)
    HDFSOperation1.mkdirs(this.writeTmp_local)
    this.writeTmp_local
  }
  def getWriteTmp_hdfs():String={
    if(!HDFSOperation1.exists(this.writeTmp_hdfs)){
      HDFSOperation1.mkdirs(this.writeTmp_hdfs)
    }
    this.writeTmp_hdfs
  }

  def getWriteTmp():String={
    this.getWriteTmp_local
    //    this.getWriteTmp_hdfs
  }
  def gethdfsWriteTmp(): String ={
    if(!HDFSOperation1.exists(this.writeTmp_hdfs)){
      HDFSOperation1.mkdirs(this.writeTmp_hdfs)
    }
    this.writeTmp_hdfs
  }


  def getTemExtremPath(): String = this.temExtremPath
  def getstationfile0():String = {
    val stnfile=this.stationfile0
    val msg="抽站地址:"+stnfile
    println(msg)
    stnfile
  }
  def getstationfile1():String = {
    val stnfile=this.stationfile1
    val msg="抽站地址:"+stnfile
    println(msg)
    stnfile
  }
  def getstationfile_stainfo_nat():String ={
    val stnfile=this.stationfile_stainfo_nat
    val msg="抽站地址:"+stnfile
    println(msg)
    stnfile
  }
  def getjsonFile(): String = this.jsonFile
  def getjsonPath():String = this.jsonPath

  private val jsonLocalPath:String=this.prop.getProperty("jsonLocalPath")
  def getjsonLocalPath():String=this.jsonLocalPath

  def getlocalStreamPath(jsonFile:String):String={
    val jsonFileName=new File(jsonFile).getName.split("\\.").head

    val localStreamRootPath:String=this.prop.getProperty("localStreamPath")

    val pathName:String={
      if(localStreamRootPath.endsWith("/")){
        localStreamRootPath+jsonFileName+"/"
      }else{
        localStreamRootPath+"/"+jsonFileName+"/"
      }
    }
    val path:File=new File(pathName)
    if(!path.exists()){
      path.mkdirs()
    }
    pathName
  }
  //  private val SplitNum=ReadFile.ReadJsonSplitNum(this.getjsonFile())

  private val typeSetRootpath_local:String=this.prop.getProperty("typeSetRootpath_local")

  /*def getJsonRdd: (RDD[JSONObject],Int) = ReadFile.ReadJsonRdd(this.jsonFile)
  def getJsonRddTest:(RDD[JSONObject],Int) = ReadFile.ReadJsonRdd(this.jsonFileTest)*/


  /*def splitNum(jsonFile:String):Int=ReadFile.ReadJsonArr(this.getjsonFile()).length
  def splitNum_local(jsonFile_local:String):Int={
    val JsonArr=ReadWrite.ReadTXT2ArrString(jsonFile_local)
    JsonArr.length
  }*/

  /*def getJsonRdd:RDD[JSONObject] = ReadFile.ReadJsonRdd(this.jsonFile)
  def getJsonArr:Array[JSONObject]= ReadFile.ReadJsonArr(this.jsonFile)*/
  /*def getJsonRdd(jsonFile:String):RDD[JSONObject] = ReadFile.ReadJsonRdd(jsonFile)
  def getJsonArr(jsonFile:String):Array[JSONObject]= ReadFile.ReadJsonArr(jsonFile)*/



  private def typeSetfileName(date:Date): String ={
    DateFormatUtil.YYYYMMDDHHStr0(date) + ".txt"
  }
  def gettypeSetfileName(date:Date,jsonFile:String):String={
    val fileName=this.typeSetfileName(date)

    //    val jsonFileName=new Path(jsonFile).getName.split("\\.").head
    val jsonFileName=new File(jsonFile).getName.split("\\.").head

    val pathName:String={
      if(this.typeSetRootpath.endsWith("/")){
        this.typeSetRootpath+jsonFileName
      }else{
        this.typeSetRootpath+"/"+jsonFileName
      }
    }


    /*if(!HDFSOperation.exists(pathName)){
      HDFSOperation.createFolder(pathName)
    }*/


    val file:String={
      if(pathName.endsWith("/")){
        pathName+fileName
      }else{
        pathName+"/"+fileName
      }
    }

    file
  }
  def gettypeSetfileName_local(date:Date,jsonFile:String):String={
    val fileName=this.typeSetfileName(date)

    val jsonFileName=new File(jsonFile).getName.split("\\.").head
    val pathName:String={
      if(this.typeSetRootpath_local.endsWith("/")){
        this.typeSetRootpath_local+jsonFileName
      }else{
        this.typeSetRootpath_local+"/"+jsonFileName
      }
    }
    pathName+"/"+fileName
  }
  def gettypeSetRootpath:String=this.typeSetRootpath
  def gettypeSetRootpath_local(jsonFile:String):String={
    val jsonFileName=new File(jsonFile).getName.split("\\.").head
    val pathName:String={
      if(this.typeSetRootpath_local.endsWith("/")){
        this.typeSetRootpath_local+jsonFileName
      }else{
        this.typeSetRootpath_local+"/"+jsonFileName
      }
    }
    pathName
  }


  /*def gettypeSetfileNamedone(date:Date):String={
    this.typeSetRootpath+"/"+DataFormatUtil.YYYYMMDDStr0(date)+"done.txt"
  }*/

  def getstreamfilepath(dataType:String,date:Date): String ={
    val dateStr = DateFormatUtil.YYYYMMDDStr0(date)
    val RootPath=this.streamRootPath
    if(RootPath.endsWith("/")){
      RootPath+dataType+"/"+dateStr+"/"
    }else{
      RootPath+"/"+dataType+"/"+dateStr+"/"
    }
    //    RootPath+"/"+dataType+"/"+dateStr+"/"
  }

  def getstreamSecPath():String = this.streamSecPath

  def getfcRootPath():String=this.fcPath

  def getfcPath(timeStep:Double,FcType:String,date:Date):String={
    this.filePath(this.fcPath,timeStep,FcType,date)
  }
  def getfcPath(FcType:String,date:Date): String ={
    this.filePath(this.fcPath,FcType,date)
  }
  def getfcPath(date:Date): String ={
    this.filePath(this.fcPath,date)
  }

  //  def getfcPath_afterObs(timeStep:Double,FcType:String,date:Date): String =this.filePath(this.afterobs_fcPath,timeStep,FcType,date)



  def getinputKVStorePath(timeStep:Double,FcType:String,date:Date): String ={
    this.filePath(this.inputKVStorePath,timeStep,FcType,date)
  }

  def getreduceLatLonPath (timeStep: Double, FcType: String, date: Date): String = {
    //    this.filePath(this.reduceLatLonPath,timeStep,FcType,date)
    this.reduceLatLonPath + "/nc/" + timeStep.toInt + "h/" + FcType + "/"
  }

  def getfcFile(timeStep:Double,FcType:String,date:Date,idName:String): String ={
    val fileName=SplitMatchInfo.oldName2newName(FcType,idName,timeStep)
    val fcpath=this.getfcPath(timeStep,FcType,date)
    if(fcpath.endsWith("/")){
      fcpath+fileName
    }else{
      fcpath+"/"+fileName
    }
  }
  /*def getfcFile_afterObs(timeStep:Double,FcType:String,date:Date,idName:String): String ={
    val fileName=SplitMatchInfo.oldName2newName(FcType,idName,timeStep)
    val fcpath=this.getfcPath_afterObs(timeStep,FcType,date)
    if(fcpath.endsWith("/")){
      fcpath+fileName
    }else{
      fcpath+"/"+fileName
    }
  }*/

  /*def getcheckOutPath(timeStep:Double,FcType:String,date:Date): String ={
    this.getcheckOutPathUnit(this.checkoutRootPath,timeStep,FcType,date)
  }*/
  def getcheckOutPath2(timeStep:Double,FcType:String,date:Date): String ={
    this.getcheckOutPath(this.checkoutRootPath2,timeStep,FcType,date)
  }
  def getcheckOutPath(checkoutRootPath:String, timeStep:Double, FcType:String, date:Date): String ={
    if(!HDFSOperation1.exists(checkoutRootPath)){
      HDFSOperation1.mkdirs(checkoutRootPath)
    }
    this.filePath(checkoutRootPath,timeStep,FcType,date)
  }

  def getcheckOutInputKVPath(timeStep:Double,FcType:String,date:Date): String ={
    this.filePath(this.inputKVCheckoutRootPath,timeStep,FcType,date)
  }

  def getcheckfileAll(checkoutRootPath:String,timeStep:Double,date:Date): String ={
    val fcType="All"
    this.filePath(checkoutRootPath,timeStep,fcType,date)
  }
  private def filePath(rootPath:String,timeStep:Double,FcType:String,date:Date):String={
    val fcPath_String=this.filePath(rootPath,FcType,date)+"/"+timeStep.toInt.toString+"h/"
    //    val fc_Path=new Path(fc_Path_String)
    if(!HDFSOperation1.exists(fcPath_String)){
      HDFSOperation1.mkdirs(fcPath_String)
    }
    fcPath_String
  }
  private def filePath(fcPath:String,FcType:String,date:Date): String ={
    val fc_Path_String=this.filePath(fcPath,date)+"/"+FcType

    if(!HDFSOperation1.exists(fc_Path_String)){
      HDFSOperation1.mkdirs(fc_Path_String)
    }
    fc_Path_String
  }
  private def filePath(fcPath:String,date:Date): String ={
    val YYYYMMDD = DateFormatUtil.YYYYMMDDStr0(date)
    val HH = DateFormatUtil.HHStr(date)
    val fc_Path_String={
      if(fcPath.endsWith("/")){
        fcPath+YYYYMMDD+"/"+HH
      }else{
        fcPath+"/"+YYYYMMDD+"/"+HH
      }
    }
    if(!HDFSOperation1.exists(fc_Path_String)){
      HDFSOperation1.mkdirs(fc_Path_String)
    }
    fc_Path_String
  }

  private def getgr2bRootPath(): String ={
    this.gr2bPath
  }
  def getgr2bFile(FcType:String,data:Date): String ={
    val path=this.getgr2bPath(FcType,data)
    /*val fileName0=DataFormatUtil.YYYYMMDDHHStr0(data)+"_"+this.getGPath(FcType)+".nc"
    val file0={
      if(path.endsWith("/")){
        path+fileName0
      }else{
        path+"/"+fileName0
      }
    }
    /*if(HDFSOperation.exists(file0)){
      file0
    }else{
      val fileName1=this.getGPath(FcType)+".nc"
      val file1=path+"/"+fileName1
      file1
    }*/
    file0*/
    this.getgr2bFile(FcType,data,path)
  }
  private def getgr2bFile(FcType:String,data:Date,path:String): String ={
    //    val path=generationFileName
    val fileName0 = DateFormatUtil.YYYYMMDDHHStr0(data) + "_" + this.getGPath(FcType) + ".nc"
    val file0=path+"/"+fileName0
    if(HDFSOperation1.exists(file0)){
      file0
    }else{
      val fileName1=this.getGPath(FcType)+".nc"
      val file1=path+"/"+fileName1
      file1
    }
  }

  def getgr2bPath(FcType:String,date:Date): String ={
    //    val date=DataFormatUtil.YYYYMMDDHHMMSS0(dateStr)
    val YYYYMMDD = DateFormatUtil.YYYYMMDDStr0(date)
    val HH = DateFormatUtil.HHStr(date)
    val Gkey=this.getGPath_pom(FcType)
    val gr2bPath={
      if(this.getgr2bRootPath.endsWith("/")){
        this.getgr2bRootPath+YYYYMMDD+"/"+HH+"/"+Gkey+"/"
      }else{
        this.getgr2bRootPath+"/"+YYYYMMDD+"/"+HH+"/"+Gkey+"/"
      }
    }
    gr2bPath
  }
  def getobsPath(obsdate:Date): String ={
    val obsdateStr = DateFormatUtil.YYYYMMDDStr0(obsdate)
    this.obsRootPath.trim+"/"+obsdateStr+"/"
  }
  def getobsFile(obsdate:Date): String ={
    val obsdateStr: String = DateFormatUtil.YYYYMMDDHHMMStr(obsdate)
    this.getobsPath(obsdate)+"/WTRGO_10m_0_0_18.160_73.446_1_0_0.01_0.01_0_0_53.561_135.099_"+obsdateStr+".nc"
  }


  private def getocfPath(ocfdate:Date,timeStep:Double):String={
    val HH = DateFormatUtil.HHStr(ocfdate)
    val YYYYMMDD = DateFormatUtil.YYYYMMDDStr0(ocfdate)
    this.ocfRootPath+"/"+timeStep.toInt+"h/"+YYYYMMDD+"/"+HH+"/"
  }
  def getocffile(ocfdate:Date,timeStep:Double): String ={
    val ocfdateStr = DateFormatUtil.YYYYMMDDHHMMStr(ocfdate)
    val fileName:String="MSP3_PMSC_OCF"+timeStep.toInt+"H_ME_L88_GLB_"+ocfdateStr+"_00000-36000.DAT"
    val path=this.getocfPath(ocfdate,timeStep)
    path+fileName
  }

  def getgr2bNcName(FcType:String,date:Date):String={
    val fcpath=this.getgr2bPath(FcType,date)
    //    val gr2bNcName= ReadFile.ReadNcName(fcpath)
    val gr2bNcName=this.getGPath(FcType)
    gr2bNcName
  }
  /*def getobsNcName(obsdate:Date):String={
    val obspath=this.getobsPath(obsdate)
    val obsNcName=ReadFile.ReadNcName(obspath)
    obsNcName
  }*/

  private val rElementHashMap_pom:java.util.HashMap[String,Array[String]]={
    val map=new java.util.HashMap[String,Array[String]]()
    this.FcTypes.foreach(
      fctype=>{
        map.put(fctype,this.prop.getProperty(fctype+"_VarName").split(","))
      }
    )
    map
  }
  private def getrElement_pom(FcType:String): Array[String] ={
    this.rElementHashMap_pom.get(FcType)
  }
  private def getrElement_json(FcType:String):Array[String]= this.rElementHashMap_json.get(FcType)

  //原始数据中的要素名字
  def getrElement(FcType:String):Array[String]={
    var rElementArr=this.getrElement_json(FcType)
    if(rElementArr == null || rElementArr.contains("")){
      rElementArr=this.getrElement_pom(FcType)
      println("FcType="+FcType+",rElementArr为空,取pom.xml中配置")
    }
    /*if(rElementArr == null){
      val e:Exception=new Exception("rElementArr为空")
      e.printStackTrace()
    }*/
    rElementArr
  }

  //写成数据中的要素名字
  def getwElement(FcType:String,timeStep:Double):Array[String]={
    if (Constant.WIND.equals(FcType)) {
      println("rename取FcType=" + Constant.WIND)
      this.getWINDSDG
    } else if (Constant.WINUV.equals(FcType)) {
      println("FcType=" + Constant.WINUV)
      this.getWINUV
    }else{
      this.getrElement_rename(FcType,timeStep)
    }
  }

  //数据重命名
  def getrElement_rename(FcType:String):Array[String]={
    if (Constant.WEATHER.equals(FcType)) {
      Array(this.weatherName)
    } else if (Constant.WIND.equals(FcType)) {
      this.getWINUV
    }else{
      val reName:String={
        val rename=this.reNameHashMap_json.get(FcType)
        if(rename == null || "".equals(rename)){
          FcType
        }else{
          rename
        }
      }
      Array(reName)
    }
  }
  def getrElement_rename(FcType:String, timeStep:Double):Array[String]={
    val reName=this.getrElement_rename(FcType)
    if(timeStep==3.0d){
      reName
    }else{
      val dataNames_alTypetimeIAs:Array[(String,String)]=this.getdataNames_alTypetimeIAs_from3h(FcType,timeStep)
      if(reName.length!=dataNames_alTypetimeIAs.length){
        dataNames_alTypetimeIAs.map(dataNames_alTypetimeIA=>(SplitMatchInfo.oldVar2newVar(dataNames_alTypetimeIA._1,dataNames_alTypetimeIA._2)))
      }else{
        reName
      }
    }
  }

  def main (args: Array[String]): Unit = {
    val timeStep: Double = 12.0d
    val dataName: String = "CLOUD_MIN"

    println(this.getNMCdataName(dataName, timeStep))

  }

  def getdataNames_alTypetimeIAs_from3h(FcType:String, timeStep:Double):Array[(String,String)]={
    //    val rElements=this.getrElement_pom(FcType)
    //    val rElements_reName=this.getrElement_rename1(FcType,timeStep)
    val rElements_reName=this.getrElement_rename(FcType)
    if(timeStep == 1.0d){
      val dataNames_alTypetimeIAs=new Array[(String,String)](rElements_reName.length)
      if (Constant.PRE.equals(FcType)) {
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.DifferenceKey)
        }
      } else if (Constant.CLOUD.equals(FcType)) {
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.Line_PercentKey)
        }
      } else if (Constant.PRS.equals(FcType)) {
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.Line_Between0Key)
        }
      }else if(Constant.VIS.equals(FcType)){
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.Line_Between1Key)
        }
      }else {
        for(i<-0 to rElements_reName.length-1){
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.LineKey)
        }
      }
      dataNames_alTypetimeIAs
    }else if(timeStep == 12.0d) {
      if (Constant.PRE.equals(FcType)) {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.SumKey)
        }
        dataNames_alTypetimeIAs
      } else if (Constant.TEM.equals(FcType) || Constant.CLOUD.equals(FcType) || Constant.RHU.equals(FcType) ) {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length * 2)
        //        var j: Int = 0
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(2 * i) = (rElements_reName(i), Constant.MinKey)
          //          j += 1
          dataNames_alTypetimeIAs(2 * i + 1) = (rElements_reName(i), Constant.MaxKey)
          //          j += 1
        }
        dataNames_alTypetimeIAs
      }else if(Constant.PRS.equals(FcType) || Constant.VIS.equals(FcType)){
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.AveKey)
        }
        dataNames_alTypetimeIAs
      } else {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.WhichMaxIndexKey)
        }
        dataNames_alTypetimeIAs
      }
    }else if(timeStep==3.0d){
      val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
      for (i <- 0 to rElements_reName.length - 1) {
        dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.LineKey)
      }
      dataNames_alTypetimeIAs
    }else{
      val e:Exception=new Exception("timeStep="+timeStep+",getdataNames_alTypetimeIAs_from3h中没有设置")
      e.printStackTrace()
      null
    }
  }
  def getdataNames_alTypetimeIAs_from1h(FcType:String, timeStep:Double):Array[(String,String)]={
    val rElements_reName=this.getrElement_rename(FcType)
    if(timeStep==3.0d){
      if (Constant.PRE.equals(FcType)) {
        val dataNames_alTypetimeIAs=new Array[(String,String)](rElements_reName.length)
        for(i<- 0 to rElements_reName.length-1){
          dataNames_alTypetimeIAs(i)=(rElements_reName(i),Constant.SumKey)
        }
        dataNames_alTypetimeIAs
      } else if (Constant.TEM.equals(FcType)) {
        val dataNames_alTypetimeIAs=new Array[(String,String)](rElements_reName.length)
        for(i<- 0 to rElements_reName.length-1){
          dataNames_alTypetimeIAs(i)=(rElements_reName(i),Constant.LineKey)
        }
        dataNames_alTypetimeIAs
      }else{
        null
      }
    }else if(timeStep==12.0d) {
      if (Constant.PRE.equals(FcType)) {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.SumKey)
        }
        dataNames_alTypetimeIAs
      } else if (Constant.TEM.equals(FcType) || Constant.CLOUD.equals(FcType) || Constant.RHU.equals(FcType)) {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length * 2)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(2*i) = (rElements_reName(i), Constant.MinKey)
          dataNames_alTypetimeIAs(2*i+1) = (rElements_reName(i), Constant.MaxKey)
        }
        dataNames_alTypetimeIAs
      } else {
        val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
        for (i <- 0 to rElements_reName.length - 1) {
          dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.AveKey)
        }
        dataNames_alTypetimeIAs
      }
    }else if(timeStep==1.0d){
      val dataNames_alTypetimeIAs = new Array[(String, String)](rElements_reName.length)
      for (i <- 0 to rElements_reName.length - 1) {
        dataNames_alTypetimeIAs(i) = (rElements_reName(i), Constant.LineKey)
      }
      dataNames_alTypetimeIAs
    }else{
      val e:Exception=new Exception("timeStep="+timeStep+",getdataNames_alTypetimeIAs_from1h中没有设置")
      e.printStackTrace()
      null
    }
  }
  def getdataNames_alTypetimeIAs_from12h(FcType:String, timeStep:Double):Array[(String,String)]={
    if(timeStep==12.0d){
      val dataNames:Array[String]=PropertiesUtil.getwElement(FcType,timeStep)
      val dataNames_alTypetimeIAs:Array[(String,String)]={
        if(dataNames.length==1){
          Array((dataNames.last,Constant.WhichFirstKey),(dataNames.last,Constant.WhichSecondKey))
        }else{
          if (FcType.equals(Constant.WIND)) {
            Array((dataNames(0),Constant.WhichFirstKey),(dataNames(0),Constant.WhichSecondKey),(dataNames(1),Constant.WhichFirstKey),(dataNames(1),Constant.WhichSecondKey))
          }else{
            dataNames.map(
              dataName=>{
                if(dataName.endsWith(Constant.MinKey)){
                  (dataName, Constant.MinKey)
                }else if(dataName.endsWith(Constant.MaxKey)){
                  (dataName,Constant.MaxKey)
                }else{
                  val e:Exception=new Exception("dataNames.length>1,dataName="+dataName+"没有对应方法")
                  e.printStackTrace()
                  null
                }
              }
            )
          }
        }
      }
      dataNames_alTypetimeIAs
    }else{
      val e:Exception=new Exception("getdataNames_alTypetimeIAs_from12h,timeStep!=12")
      e.printStackTrace()
      null
    }
  }

  private def getWINDSD:Array[String]=this.prop.getProperty("WINDSD_VarName_write0").split(",")
  private def getWINDSDG:Array[String]=this.prop.getProperty("WINDSD_VarName_write1").split(",")
  private def getWINUV:Array[String]=this.prop.getProperty("WINDSD_VarName_write2").split(",")

  def WINU:String=this.getWINUV(0)
  def WINV:String=this.getWINUV(1)
  def WINS:String=this.getWINDSD(0)
  def WIND:String=this.getWINDSD(1)
  def WINSG:String=this.getWINDSDG(0)
  def WINDG:String=this.getWINDSDG(1)

  private def getGPath_pom(FcType:String):String={
    this.GPathHashMap_pom.get(FcType)
  }
  private def getGPath_json(FcType: String):String={
    this.GPathHashMap_json.get(FcType)
  }
  def getGPath(FcType: String):String={
    if (Constant.WEATHER.equals(FcType)) {
      this.weatherName
    }else{
      var GPath=this.GPathHashMap_json.get(FcType)
      val msg="GPath="+GPath
      if("".equals(GPath) || GPath==null){
        GPath=this.GPathHashMap_pom.get(FcType)
        println(msg+",未获取上游数据Json信息,使用pom.xml中配置信息,GPath="+GPath)
      }
      GPath
    }
  }


  def getFcTypeRadioOffset(fcType:String):(Double,Double)={
    val RadioOffset:Array[String]={
      if (Constant.TEM.equals(fcType)) {
        this.prop.getProperty("TEM_RadioOffset").split(",")
      } else if (Constant.PRS.equals(fcType)) {
        this.prop.getProperty("PRS_RadioOffset").split(",")
      }else{
        Array("1.0d","0.0d")
      }
    }
    val radio:Double=RadioOffset(0).toDouble
    val offset:Double=RadioOffset(1).toDouble
    (radio,offset)
  }
  def getKVStoreType(timeStep:Double,idName:String): String ={
    this.getKVStoreType0(timeStep,idName)
  }
  private def getKVStoreType0(timeStep:Double,idName:String):String={
    if(timeStep==1.0d) {
      "N1H"
    } else if (timeStep == 12.0d || timeStep == 24.0d) {
      "N12H"
    }else{
      val e:Exception=new Exception("timeStep="+timeStep+"没有入库设置")
      e.printStackTrace()
      null
    }
  }

  //  private def getKVStoreType1(timeStep:Double,idName:String):String={
  //    val idNames0:Array[String]=Array("A49","B50","C50","D50","E49","E51","F48","F50","G45","G47","G49","G51","H44","H46","H48","H50","H52","I44","I46","I48","I50","I52","J44","J46","J48","J50","K43","K45","K47","K49","K51","L44","L46","L50","L52","M45","M51","M53","N52")
  //    val idNames1:Array[String]=Array("B49","C49","D49","E48","E50","F47","F49","F51","G46","G48","G50","G52","H45","H47","H49","H51","I43","I45","I47","I49","I51","J43","J45","J47","J49","J51","K44","K46","K48","K50","K52","L45","L49","L51","L53","M50","M52","N51")
  //    if(timeStep==1.0d) {
  //      if(idNames0.contains(idName)){
  //        "N" + timeStep.toInt.toString + "H0"
  //      }else if(idNames1.contains(idName)){
  //        "N" + timeStep.toInt.toString + "H1"
  //      }else{
  //        ""
  //      }
  //    }else if(timeStep==12.0d){
  //      "N" + timeStep.toInt.toString + "H"
  //    }else{
  //      val e:Exception=new Exception("timeStep="+timeStep+"没有入库设置")
  //      e.printStackTrace()
  //      null
  //    }
  //  }

  def getKVStoreURL():String=this.prop.getProperty("KVStoreURL")
  def getKVStoreKey(dataName:String,timeStep:Double): String ={
    val key:String={
      if(dataName.endsWith(Constant.MinKey)) {
        SplitMatchInfo.newVar2oldVar(dataName)+"MIN"
      }else if(dataName.endsWith(Constant.MaxKey)){
        SplitMatchInfo.newVar2oldVar(dataName)+"MAX"
      }else{
        if(timeStep == 1.0d){
          dataName
        } else if (timeStep == 12.0d || timeStep == 24.0d) {
          if(dataName.endsWith(Constant.WhichFirstKey)){
            SplitMatchInfo.newVar2oldVar(dataName)+"1"
          }else if(dataName.endsWith(Constant.WhichSecondKey)){
            SplitMatchInfo.newVar2oldVar(dataName)+"2"
          }else{
            val e:Exception=new Exception("dataName="+dataName+";timeStep="+timeStep+"没有匹配的入库key")
            e.printStackTrace()
            null
          }
        }else{
          val e:Exception=new Exception("dataName="+dataName+";timeStep="+timeStep+"没有匹配的入库key")
          e.printStackTrace()
          null
        }
      }
    }
    println("KVStoreKey(dataName="+dataName+",timeStep="+timeStep+")--->data.key="+key)
    key
  }
  def getKVtimeOut(): Int ={
    val timeout: Int=this.prop.getProperty("KVStoretimeOut").toInt
    timeout
  }

  def getNMCdataName (dataName: String, timeStep: Double): String = {
    val dataName0: String = {
      if (timeStep == 1.0d) {
        dataName
      } else if (timeStep == 12.0d) {
        if (dataName.contains("_")) {
          dataName.replace("_", "")
          //          dataName.filter(p=>(!p.equals("_")))
        } else {
          dataName
        }
      } else {
        val e: Exception = new Exception("未配置getNMCdataName中timeStep=" + timeStep)
        e.printStackTrace( )
        dataName
      }
    }
    val dataName1: String = {
      dataName0.toUpperCase
    }
    dataName1
  }

  /*private def getGeoPath():String=this.geoPath*/
  def getGeoFile():String=this.geoFile
  def getOcfFileNum(FcType: String):Int={
    this.prop.getProperty(FcType+"_Ocf_num").toInt
  }

  def putPropertiesHashMap_json(fcType: String, attribute:String):Unit={
    val attributejson=JSON.parseObject(attribute)

    //    val attributejson=JO.str2JsonObject(attribute)
    try{
      //上游数据中的要素信息
      val elestr=attributejson.getString("ele")
      val elearrs=JSON.parseArray(elestr).toArray()
      val eleArr:Array[String]=elearrs.map(
        eleObj=>(eleObj.toString)
      )
      this.rElementHashMap_json.put(fcType,eleArr)
    }catch{
      case e: Exception => {
        val e:Exception=new Exception("解析上游数据中的要素信息时出错")
        e.printStackTrace()
      }
    }
    try{
      //time,height,lat,lon信息
      val timeName=attributejson.getString("time")
      val heightName=attributejson.getString("height")
      val latName=attributejson.getString("lat")
      val lonName=attributejson.getString("lon")
      this.VarNameMap.put("timeName",timeName)
      this.VarNameMap.put("heightName",heightName)
      this.VarNameMap.put("latName",latName)
      this.VarNameMap.put("lonName",lonName)
    }catch{
      case e: Exception => {
        val e:Exception=new Exception("解析time,height,lat,lon信息时出错")
        e.printStackTrace()
      }
    }
    try{
      //要素文件名信息
      val GPath=attributejson.getString("key")
      this.GPathHashMap_json.put(fcType,GPath)
    }catch{
      case e: Exception => {
        val e:Exception=new Exception("解析要素文件名信息时出错")
        e.printStackTrace()
      }
    }

    try{
      val reName:String=attributejson.getString("rename")
      this.reNameHashMap_json.put(fcType,reName)
    }catch{
      case e:Exception =>{
        val e:Exception=new Exception("解析reName信息时出错")
        e.printStackTrace()
      }
    }
  }

  def getLatLonStr(latlon:String=this.latlon):(Double,Double,Double,Double)={
    val LatLonSplit = latlon.split(",")
    val startLat=LatLonSplit(0).toDouble
    val endLat=LatLonSplit(1).toDouble
    val startLon=LatLonSplit(2).toDouble
    val endLon=LatLonSplit(3).toDouble
    val minLat=Math.min(startLat,endLat)
    val maxLat=Math.max(startLat,endLat)
    val minLon=Math.min(startLon,endLon)
    val maxLon=Math.max(startLon,endLon)

    (minLat,maxLat,minLon,maxLon)
  }
  def getLatLon(latstep:Double,lonstep:Double,latlon:String):(Array[Double],Array[Double])={
    val (minLat,maxLat,minLon,maxLon)=this.getLatLonStr(latlon)
    val Lat = NumOperation.Kdec(ArrayOperation.ArithmeticArray(minLat, maxLat, latstep))
    val Lon = NumOperation.Kdec(ArrayOperation.ArithmeticArray(minLon, maxLon, lonstep))
    (Lat,Lon)
  }

  def getLatLon(latlonstep:Double,latlon:String=this.latlon): (Array[Double],Array[Double]) =this.getLatLon(latlonstep,latlonstep,latlon)
  //  def getLatLon(latlonstep:Double=0.01d,latlon:String=this.latlon): (Array[Double],Array[Double]) =this.getLatLon(latlonstep,latlonstep,latlon)

  private val FcTypeAttributeMap:java.util.HashMap[String,String]={
    //    val FcTypes=Array(Constant.PRE,Constant.TEM,Constant.RHU,Constant.WIND,Constant.CLOUD,Constant.PRS)
    val map=new java.util.HashMap[String,String]
    this.FcTypes.foreach(
      FcType=>{
        val attr=this.prop.getProperty(FcType+"_Attribute")
        map.put(FcType,attr)
      }
    )
    map
  }
  def getFcTypeAttrbute(fcType:String):String={
    this.FcTypeAttributeMap.get(fcType)
  }
  def getFcTypedataType(fcType:String): String ={
    val fcTypes = Constant.OrdinaryFcTypes
    if(fcTypes.contains(fcType)){
      "Gr2b_"+fcType
    }else{
      println("fcType="+fcType+",未配置dataType")
      null
    }
  }

  def gettimes(timeStep:Double):Array[Double] ={
    val times:Array[Double]={
      if(timeStep==1){
        ArrayOperation.ArithmeticArray(1.0d, 72.0d, 1.0d)
      }else if(timeStep==3){
        ArrayOperation.ArithmeticArray(3.0d, 240.0d, 3.0d)
      }else if(timeStep==12){
        ArrayOperation.ArithmeticArray(12.0d, 240.0d, 12.0d)
      }else{
        val e:Exception=new Exception("参数timeStep="+timeStep+";不存在times")
        e.printStackTrace()
        null
      }
    }
    times
  }
  def gettimes_Split(timeStep:Double): Array[(Double,Array[Double])] ={
    val Split:Int={
      if(timeStep==1){
        //72
        3
      }else if(timeStep==3){
        //80
        2
      }else if(timeStep==12){
        //20
        1
      }else{
        val e:Exception=new Exception("参数timeStep="+timeStep+";不存在对应timesSplitNum")
        e.printStackTrace()
        0
      }
    }
    val times=this.gettimes(timeStep)
    val timesLen:Int=times.length
    val timeLen0:Int= timesLen / Split
    val timesArr:Array[(Double,Array[Double])]=new Array[(Double,Array[Double])](Split)
    for(i<- 0 to Split-1){
      val timeArray:Array[Double]=new Array[Double](timeLen0)
      for(j<- 0 to timeLen0-1){
        timeArray(j)=times(i*timeLen0+j)
      }
      timesArr(i)=(timeStep,timeArray)
    }
    timesArr
  }

  /*private val DataNameDataTypeMap0:mutable.HashMap[String,String]={
    val map0:mutable.HashMap[String,String]=new mutable.HashMap[String,String]()
    map0.put("CLOUD","float")
    map0.put("CLOUD_Max","float")
    map0.put("CLOUD_Min","float")
    map0.put("WEATHER","byte")
    map0.put("PRE","float")
    map0.put("PRS","float")
    map0.put("PRS_Min","float")
    map0.put("PRS_Max","float")
    map0.put("TEM","float")
    map0.put("TEM_Min","float")
    map0.put("TEM_Max","float")
    map0.put("RHU","float")
    map0.put("RHU_Min","float")
    map0.put("RHU_Max","float")
    map0.put("WIND","byte")
    map0.put("WINS","byte")
    map0
  }
  def dataTypeFromdataName(dataName:String): String ={
    this.DataNameDataTypeMap0.get(dataName).get
  }*/

  private def reduceLatLonDataNameDataTypeMap1: mutable.HashMap[String, (String, Int)] = {
    val map0: mutable.HashMap[String, (String, Int)] = new mutable.HashMap[String, (String, Int)]( )

    val varNames: (String, String, String, String) = this.getVarName( )
    val latName: String = varNames._3
    val lonName: String = varNames._4
    map0.put(latName, ("float", 0))
    map0.put(lonName, ("float", 0))

    map0.put(Constant.CLOUD, ("byte", 0)) //3
    map0.put(Constant.WEATHER, ("byte", 0)) //3
    map0.put(Constant.PRE, ("short", 1)) //short,3
    map0.put(Constant.PRS, ("short", 0)) //short,3
    map0.put(Constant.TEM, ("short", 1)) //short,3
    map0.put(Constant.RHU, ("byte", 0)) //3
    map0.put(Constant.WIND, ("byte", 0)) //3
    map0.put(Constant.VIS, ("short", 0)) //3
    map0.put(Constant.WINUV, ("short", 0))
    map0
  }

  def reduceLatLon_DataTypeRadio (fcType: String): (String, Int) = {
    val dataTypes_radio = this.reduceLatLonDataNameDataTypeMap1.get(fcType).get
    val dataTypes = dataTypes_radio._1
    val radio = dataTypes_radio._2
    (dataTypes, radio)
  }
  val latlonStep: Double = 0.01d
}
