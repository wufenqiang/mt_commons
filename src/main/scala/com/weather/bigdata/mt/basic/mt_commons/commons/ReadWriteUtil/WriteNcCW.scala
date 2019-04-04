package com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil

import java.text.SimpleDateFormat

import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesOperationCW, WriteSciCW}
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.VariableOperationCW
import com.weather.bigdata.it.utils.formatUtil.StringDate
import com.weather.bigdata.it.utils.hdfsUtil.HDFSOperation1
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType
import ucar.nc2.Dimension

import scala.collection.mutable

object WriteNcCW {

  def WriteNcFile_test (scidata: SciDatasetCW, resultHdfsPath: String): Boolean = {
    val ws=WriteSciCW.fromSciCW(scidata)
    val flag = ws.writeHDFS(resultHdfsPath)
    flag
  }

  /*private def WriteSci (scidata0: SciDataset, resultHdfsPath: String): Boolean = {
    //获取ExcutorID
    val scidata = AttributesOperation.addExecutorIDTrace(scidata0)
    val func = SciFunctions.fromRDD(scidata)
    val tmp = PropertiesUtil.getWriteTmp
    val flag=func.writeSRDD(resultHdfsPath, tmp)
    flag
  }*/

  def WriteNcFile_test (rdd: RDD[SciDatasetCW], resultHdfsPath: String): Unit = {
    this.WriteNcFileReturn(rdd, resultHdfsPath)
  }

  /*private def WriteSci(fileRdd: RDD[SciDataset], resultHdfsPath: String): Unit = {
    val func = SRDDFunctions.fromRDD(fileRdd)
    val tmp=PropertiesUtil.getWriteTmp
    func.writeSRDD(resultHdfsPath,tmp)
  }*/
  /*private def WriteNcFileAction (fileRdd: RDD[SciDataset], resultHdfsPath: String): Unit = {
    fileRdd.foreach(scidata => this.WriteSci(scidata, resultHdfsPath))
  }*/

  /**
    * 写文件到hdfs<br/>
    * 目前只支持nc格式<br/>
    *
    * @param fileRdd        文件rdd:RDD[SciDataset]
    * @param resultHdfsPath 需要写文件的全hdfs路径 如/data/scispark/nmc/wind/ret/
    */
  private def WriteNcFileReturn (fileRdd: RDD[SciDatasetCW], resultHdfsPath: String): Boolean = {
    fileRdd.map(scidata => {
      val ws=WriteSciCW.fromSciCW(scidata)
      ws.writeHDFS(resultHdfsPath)
    }).reduce((x, y) => (x && y))
  }

  def WriteNcFile_addID_tmpAction (fileRdd: RDD[SciDatasetCW], resultHdfsPath: String, appID: String): Unit = {
    fileRdd.foreach(scidata => this.WriteNcFile_addID_tmp(scidata, resultHdfsPath, appID))
  }

  def WriteNcFile_addID_tmp (scidata: SciDatasetCW, resultHdfsPath: String, appID: String): Boolean = {
    val oldName: String = scidata.datasetName
    val newName: String = appID+oldName
    scidata.setName(newName)
    val flag = this.WriteNcFile_addID(scidata, resultHdfsPath, appID)
    if (flag) {
      val oldFile: String = resultHdfsPath + oldName
      val newFile: String = resultHdfsPath + newName
      val reName = HDFSOperation1.renamefile(newFile, oldFile)
      if (!reName) {
        val msg = "WriteNcFile_addID_tmp[reName]出错!"
        val e: Exception = new Exception(msg)
        e.printStackTrace( )
        false
      }
      reName
    } else {
      val msg = "WriteNcFile_addID_tmp[WriteSci]出错!"
      val e: Exception = new Exception(msg)
      e.printStackTrace( )
      false
    }
  }

  private def WriteNcFile_addID (scidata: SciDatasetCW, resultHdfsPath: String, appID: String): Boolean = {
    val addAppIDscidata = AttributesOperationCW.addAppIDTrace(scidata, appID)
    val ws=WriteSciCW.fromSciCW(addAppIDscidata)
    ws.writeHDFS(resultHdfsPath)
  }

  def WriteNcFile_addID_tmpReturn (fileRdd: RDD[SciDatasetCW], resultHdfsPath: String, appID: String): Boolean = {
    fileRdd.map(scidata => this.WriteNcFile_addID_tmp(scidata, resultHdfsPath, appID)).reduce((x, y) => (x & y))
  }

  def WriteFcFile_addId_tmpReturn (fileRdd: RDD[SciDatasetCW], appID: String): Boolean = {
    fileRdd.map(scidata => this.WriteFcFile_addId_tmp(scidata, appID)).reduce((x, y) => (x && y))
  }

  def WriteFcFile_addId_tmp (scidata: SciDatasetCW, appID: String): Boolean = {
    val fcdate = AttributesOperationCW.getfcdate(scidata)
    val fcType = AttributesOperationCW.getFcType(scidata)
    val timeStep = AttributesOperationCW.gettimeStep(scidata)
    val fcPath = PropertiesUtil.getfcPath(timeStep, fcType, fcdate)
    val flag = this.WriteNcFile_addID_tmp(scidata, fcPath, appID)


    //-------------------------------------------日志采集----------------------------------------------------
    //    val targetFileStr0 = fcPath + scidata.datasetName
    //    val targetFileStr = SplitMatchInfo.reduceAppId(targetFileStr0, appID)
    //    //    val changePerson: String = ConstantUtil.fcType_ChangePerson
    //    var basicRemark: JSONObject = new JSONObject( )
    //    basicRemark = AttributesOperation.addAppId(basicRemark, appID)
    //    basicRemark = AttributesOperation.addAsyOpen(basicRemark)
    //    basicRemark = AttributesOperation.addTimeStep(basicRemark, timeStep)
    //    basicRemark = AttributesOperation.addFcType(basicRemark, fcType)
    //    val generateStr: String = {
    //      if (flag) {
    //        "Y"
    //      } else {
    //        "N"
    //      }
    //    }
    //    val sizeFlagStr: String = "Null"
    //    val delayStr: String = "Null"
    //    val idName = AttributesOperationCW.getIDName(scidata)
    //    var stateRemark: JSONObject = new JSONObject( )
    //    stateRemark = AttributesOperation.addIDName(stateRemark, idName)
    //    stateRemark = AttributesOperation.addAppId(stateRemark, appID)
    //    SendMsg.productSend(targetFileStr, Constant.fcType_ChangePerson, basicRemark, generateStr, sizeFlagStr, delayStr, stateRemark)
    //-------------------------------------------日志采集----------------------------------------------------

    flag
  }

  def WriteFcFile_addId_tmpAction (fileRdd: RDD[SciDatasetCW], appID: String): Unit = {
    fileRdd.foreach(scidata => this.WriteFcFile_addId_tmp(scidata, appID))
  }

  /*def WriteFcFile_addId_tmp_afterObs(scidata:SciDataset,appID:String): Boolean ={
    val fcdate=AttributesOperation.getfcdate(scidata)
    val fcType=AttributesOperation.getFcType(scidata)
    val timeStep=AttributesOperation.gettimeStep(scidata)
    val fcPath_afterObs = PropertiesUtil.getfcPath_afterObs(timeStep,fcType,fcdate)
    val flag=WriteFile.WriteNcFile_addID_tmp(scidata,fcPath_afterObs,appID)


    //-------------------------------------------日志采集----------------------------------------------------
    val targetFileStr0 = fcPath_afterObs + scidata.datasetName
    val targetFileStr=SplitMatchInfo.reduceAppId(targetFileStr0,appID)
    val changePerson:String=ConstantUtil.fcType_ChangePerson
    var basicRemark:JSONObject=new JSONObject()
    basicRemark=AttributesOperation.addAppId(basicRemark,appID)
    basicRemark=AttributesOperation.addAsyOpen(basicRemark)
    basicRemark=AttributesOperation.addTimeStep(basicRemark,timeStep)
    basicRemark=AttributesOperation.addFcType(basicRemark,fcType)
    val generateStr:String={
      if(flag){
        "Y"
      }else{
        "N"
      }
    }
    val sizeFlagStr:String="Null"
    val delayStr:String="Null"
    val idName = AttributesOperation.getIDName(scidata)
    var stateRemark:JSONObject=new JSONObject()
    stateRemark=AttributesOperation.addIDName(stateRemark,idName)
    stateRemark=AttributesOperation.addAppId(stateRemark,appID)
    SendMsg.productSend(targetFileStr,changePerson,basicRemark,generateStr,sizeFlagStr,delayStr,stateRemark)
    //-------------------------------------------日志采集----------------------------------------------------

    flag
  }*/

  def main (args: Array[String]): Unit = {

  }

  private def write (scidata0: SciDatasetCW, resultHdfsPath: String, reftimeOpen: Boolean): Boolean = {
    if (reftimeOpen) {
      val variables: mutable.HashMap[String, VariableCW] = scidata0.variables

      val reftimeKey = "reftime"
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
      val fcdate = AttributesOperationCW.getfcdate(scidata0)
      val timeStep = AttributesOperationCW.gettimeStep(scidata0)
      val timeName = PropertiesUtil.getVarName()._1
      val timeV = variables.get(timeName).get
      val timeLen = timeV.getLen( )
      val times = ArrayOperation.ArithmeticArray(timeStep, timeLen, timeStep)
      val dims_add: java.util.ArrayList[Dimension] = new java.util.ArrayList[Dimension]( )
      val sd: scala.Array[String] = StringDate.ArithmeticArrayDate_Hour(sdf, fcdate, times)

      val array_add: ucar.ma2.Array = ucar.ma2.Array.factory(DataType.STRING, scala.Array(timeLen), sd)

      val reftimeVariable = VariableOperationCW.creatVar(array_add, reftimeKey)

      variables.put(reftimeKey, reftimeVariable)
    }
    val ws=WriteSciCW.fromSciCW(scidata0)
    ws.writeHDFS(resultHdfsPath)
  }

  /*  private def WriteNMCNcFile(fileRdd: RDD[SciDataset]): RDD[Boolean] ={
      fileRdd.map(scidata=>this.WriteNMCNcFile(scidata))
    }
    private def WriteNMCNcFile(scidata:SciDataset): Boolean ={
      val fcType=AttributesOperation.getFcType(scidata)
      val timeStep=AttributesOperation.gettimeStep(scidata)
      val idName=AttributesOperation.getIDName(scidata)
      val fcdate=AttributesOperation.getfcdate(scidata)
      val resultHdfsPath:String=PropertiesUtil.getfcPath(timeStep,fcType,fcdate)
      val flag=this.WriteSci(scidata,resultHdfsPath)

      val fileName=SplitMatchInfo.oldName2newName(fcType,idName,timeStep)
      val file=resultHdfsPath+"/"+fileName
      if(flag){
        val msg="数据生成成功:"+file
        print(msg)
      }else{
        val msg="数据生成失败:"+file
        val e:Exception=new Exception(msg)
        e.printStackTrace()
      }
      flag
    }*/

  /*  private def WriteNcFile_id_fcType_timeStep(scidata:SciDataset,resultHdfsPath:String): Unit ={
      val func = SciFunctions.fromRDD(scidata)
      val tmp=PropertiesUtil.getWriteTmp
      func.writeSRDD_id_fcType_timeStep(resultHdfsPath,tmp)
    }
    private def WriteNcFile_id_fcType_timeStep(fileRdd: RDD[SciDataset], resultHdfsPath: String): Unit ={
      fileRdd.foreach(f=>this.WriteNcFile_id_fcType_timeStep(f,resultHdfsPath))
    }*/

  /*def WriteStr2HDFS(fileName:String,content:String): Unit ={
    val rw=new ReadWrite
    val file=new File(fileName)
    file.getName
    val tmpPathStr=PropertiesUtil.getWriteTmp()

    rw.WriteTXT1(fileName:String,content:String)

  }*/

  /*  def WriteNcFile_addID_tmp(fileRdd: RDD[SciDataset], resultHdfsPath:String, appID:String):Boolean={
      val resultHdfsPathtmp=PathUtil.path2tmp(resultHdfsPath,appID)
      val flag=fileRdd.map(scidata=>this.WriteNcFile_addID(scidata,resultHdfsPathtmp,appID)).reduce((x,y)=>(x && y))

      if(flag){
        val renameflag=HDFSOperation.reName(resultHdfsPathtmp,resultHdfsPath)
        if(!renameflag){
          val e:Exception=new Exception("reName失败")
          e.printStackTrace()
        }
      }
      flag
    }
    def WriteNcFile_addID_tmp(scidata:SciDataset,resultHdfsPath:String,appID:String):Boolean={
      val resultHdfsPathtmp=PathUtil.path2tmp(resultHdfsPath,appID)
      val addAppIDscidata=AttributesOperation.addAppIDTrace(scidata,appID)
      this.WriteSci(addAppIDscidata,resultHdfsPathtmp)
    }

    def WriteNcFile_addID_tmp2path(flag:Boolean,timeSteps:Array[Double],fcTypes:Array[String],appID:String,date:Date): Unit ={
      if(flag){
        timeSteps.foreach(timeStep=>{
          fcTypes.foreach(fcType=>{
              val fcPath = PropertiesUtil.getfcPath(timeStep,fcType,date)
              val fcPathtmp=PathUtil.path2tmp(fcPath,appID)
              HDFSOperation.reName(fcPathtmp,fcPath)
          })
        })
      }
    }
    def WriteNcFile_addID_tmp2path(flag:Boolean,timeSteps:Array[Double],fcType:String,appID:String,date:Date): Unit ={
      this.WriteNcFile_addID_tmp2path(flag,timeSteps,Array(fcType),appID,date)
    }
    def WriteNcFile_addID_tmp2path(flag:Boolean,timeStep:Double,fcTypes:Array[String],appID:String,date:Date): Unit ={
      this.WriteNcFile_addID_tmp2path(flag,Array(timeStep),fcTypes,appID,date)
    }
    def WriteNcFile_addID_tmp2path(flag:Boolean,timeStep:Double,fcType:String,appID:String,date:Date): Unit ={
      this.WriteNcFile_addID_tmp2path(flag,Array(timeStep),Array(fcType),appID,date)
    }*/
  private def WriteNcFile_addID (fileRdd: RDD[SciDatasetCW], resultHdfsPath: String, appID: String): Boolean = {
    fileRdd.map(scidata => this.WriteNcFile_addID(scidata, resultHdfsPath, appID)).reduce((x, y) => (x && y))
  }
}
