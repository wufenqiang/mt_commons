package com.weather.bigdata.mt.basic.mt_commons.commons.PheUtil

import com.weather.bigdata.it.nc_grib.ReadWrite.AttributesOperationCW
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.{IndexOperation, VariableOperationCW, ucarma2ArrayOperationCW}
import com.weather.bigdata.it.utils.operation.ArrayOperation
import com.weather.bigdata.mt.basic.mt_commons.commons.SplitUtil.{MatchSciDatasCW, SplitMatchInfo}
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD
import ucar.ma2.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object PheCW {
  private val weaDataType: String = "byte"

  /**
    * 天气现象计算方法
    * Created by lijing on 2018/02/08.
    * Edited by wufenqiang on 2018/04/13
    * reEdited by wufenqiang on 2019/02/14
    */
  def Weather_Wea0(jsonFile: String, PRErdd: RDD[SciDatasetCW], TEMrdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    this.WeatherBy_pre_tem_cld1(jsonFile, PRErdd, TEMrdd, CLOUDrdd, timeName, heightName, latName, lonName, timeStep, weatherName)
  }

  private def WeatherBy_pre_tem_cld1(jsonFile: String, PRErdd: RDD[SciDatasetCW], TEMrdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    val alls_KV = MatchSciDatasCW.matchByName1(Array(PRErdd, TEMrdd, CLOUDrdd), jsonFile)
    val wea_rdd = alls_KV.map(
      all => {
        val PREname = Constant.PRE
        val TEMname = Constant.TEM
        val CLOUDname = Constant.CLOUD

        val idName = all._1
        val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)
        val idPREname = SplitMatchInfo.oldName2newName(PREname, idName, timeStep)
        val idTEMname = SplitMatchInfo.oldName2newName(TEMname, idName, timeStep)
        val idCLOUDname = SplitMatchInfo.oldName2newName(CLOUDname, idName, timeStep)

        val PRES: SciDatasetCW = all._2.find(p => p.datasetName.equals(idPREname)).get
        val CLOUDS: SciDatasetCW = all._2.find(p => p.datasetName.equals(idCLOUDname)).get
        val TEMS: SciDatasetCW = all._2.find(p => p.datasetName.equals(idTEMname)).get

        this.WeatherBy_pre_tem_cld1(PRES, TEMS, CLOUDS, timeName, heightName, latName, lonName, timeStep, weatherName)
      })
    wea_rdd
  }

  private def WeatherBy_pre_tem_cld1(PREsci: SciDatasetCW, TEMsci: SciDatasetCW, CLOUDsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): SciDatasetCW = {
    val idName_PRE = AttributesOperationCW.getIDName(PREsci)
    val idName_TEM = AttributesOperationCW.getIDName(TEMsci)
    val idName_CLOUD = AttributesOperationCW.getIDName(CLOUDsci)
    val idName: String = {
      if (idName_PRE.equals(idName_TEM) && idName_PRE.equals(idName_CLOUD)) {
        idName_PRE
      } else {
        val e: Exception = new Exception("WeatherBy_pre_tem_cld1,idName匹配出错")
        e.printStackTrace()
        "-1"
      }
    }


    val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)
    val PREname = PropertiesUtil.getrElement_rename(Constant.PRE)(0)
    val CLOUDname = PropertiesUtil.getrElement_rename(Constant.CLOUD, timeStep)(0)
    val TEMname = PropertiesUtil.getrElement_rename(Constant.TEM, timeStep)(0)

    //    val PREname=PropertiesUtil.PREname
    //    val TEMname=PropertiesUtil.TEMname
    //    val CLOUDname=PropertiesUtil.CLOUDname

    val PREV: VariableCW = PREsci.apply(PREname)
    val CLOUDV: VariableCW = CLOUDsci.apply(CLOUDname)
    val TEMV: VariableCW = TEMsci.apply(TEMname)

    val timeV = PREsci.apply(timeName)
    val heightV = PREsci.apply(heightName)
    val latV = PREsci.apply(latName)
    val lonV = PREsci.apply(lonName)


    val weaVar: VariableCW = this.getWea0Variable1(weatherName, timeStep, PREV, TEMV, CLOUDV, timeV, heightV, latV, lonV)

    val vars = new ArrayBuffer[VariableCW]()
    vars += timeV += heightV += latV += lonV += weaVar
    //    val varHash=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))

    val preAtt = PREsci.attributes
    val cldAtt = CLOUDsci.attributes
    val temAtt = TEMsci.attributes

    val fcDate = AttributesOperationCW.getfcdate(preAtt)

    var att = new mutable.HashMap[String, String]
    att = AttributesOperationCW.addTrace(att, "WeatherBy_pre_tem_cld1")
    att = AttributesOperationCW.updateFcType(att, Constant.WEATHER)
    att = AttributesOperationCW.updateTimeStep(att, timeStep)
    att = AttributesOperationCW.addIDName(att, idName)
    att = AttributesOperationCW.updatefcdate(att, fcDate)
    att = AttributesOperationCW.addVarName(att, (timeName, heightName, latName, lonName))

    new SciDatasetCW(vars, att, AreaName)
  }

  /**
    * Edited by wufenqiang on 2018/04/13
    *
    * @param weatherName
    * @param timeStep
    * @param PREV
    * @param TEMV
    * @param CLOUDV
    * @param timeV
    * @param heightV
    * @param latV
    * @param lonV
    * @return
    */
  private def getWea0Variable1(weatherName: String, timeStep: Double, PREV: VariableCW, TEMV: VariableCW, CLOUDV: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW): VariableCW = {
    //    val pre: Array[Double] = PREV.data
    //    val tem=TEMV.data
    //    val cld=CLOUDV.data

    val timeName = timeV.name
    val heightName = heightV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeDims = timeV.getLen()
    val heightDims = heightV.getLen()
    val latDims = latV.getLen()
    val lonDims = lonV.getLen()

    val shape = Array(timeDims, heightDims, latDims, lonDims)
    val WEA = ucar.ma2.Array.factory(DataType.getType(this.weaDataType), shape)

    val length = timeDims * heightDims * latDims * lonDims
    //    val data = new Array[Double](length)

    val (rainlevel, snowlevel) = PheRule.Phe_level0(timeStep)

    for (i <- 0 until length) {
      //晴雨判断结束
      ucarma2ArrayOperationCW.setObject(WEA, i, PheRule.v0(PREV.dataDouble(i), TEMV.dataDouble(i), CLOUDV.dataDouble(i), rainlevel, snowlevel))
    }
    VariableOperationCW.creatVars4(WEA, weatherName, timeName, heightName, latName, lonName)
    //    VariableOperation.creatVars4(pre, weatherName, "byte", timeName, heightName, latName, lonName, timeDims, heightDims, latDims, lonDims)
  }

  def Weather_Wea0(PREsci: SciDatasetCW, TEMsci: SciDatasetCW, CLOUDsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): SciDatasetCW = {
    this.WeatherBy_pre_tem_cld1(PREsci, TEMsci, CLOUDsci, timeName, heightName, latName, lonName, timeStep, weatherName)
  }

  /*private def getWea0Variable0(weatherName:String,timeStep:Double,PREV:Variable,TEMV:Variable,CLOUDV:Variable,timeV:Variable,heightV:Variable,latV:Variable,lonV:Variable): Variable ={
    val pre=PREV.data
    val tem=TEMV.data
    val cld=CLOUDV.data

    val timeName=timeV.name
    val heightName=heightV.name
    val latName=latV.name
    val lonName=lonV.name

    val timeDims =timeV.data.length
    val heightDims =heightV.data.length
    val latDims =latV.data.length
    val lonDims =lonV.data.length

    val length = timeDims * heightDims * latDims * lonDims
    val data = new Array[Double](length)

    val rainlevel1 = Array(2.5f, 8.0f, 16.0f, 40.0f, 80.0f)
    val snowlevel1 = Array(0.4f, 1.4f, 3.0f, 4.0f, 5.0f)
    val rainlevel3 = Array(3.0f, 10.0f, 20.0f, 50.0f, 100.0f)
    val snowlevel3 = Array(0.5f, 1.5f, 4.0f, 7.0f, 10.0f)
    val rainlevel6 = Array(4.0f, 13.0f, 25.0f, 60.0f, 120.0f)
    val snowlevel6 = Array(0.7f, 2.0f, 5.0f, 8.0f, 12.0f)
    val rainlevel12 = Array(10.0f, 25.0f, 50.0f, 100.0f, 250.0f)
    val snowlevel12 = Array(2.5f, 5.0f, 10.0f, 20.0f, 30.0f)
    var rainlevel = Array(10.0f, 25.0f, 50.0f, 100.0f, 250.0f)
    var snowlevel = Array(2.5f, 5.0f, 10.0f, 20.0f, 30.0f)

    /*//按时间分辨率引用降水量分级标准
    if (TimRes.equals("fc_1h")) {
      rainlevel = rainlevel1
      snowlevel = snowlevel1
    }else if (TimRes.equals("fc_3h")) {
      rainlevel = rainlevel3
      snowlevel = snowlevel3
    }else if (TimRes.equals("fc_12h")) {
      rainlevel = rainlevel12
      snowlevel = snowlevel12
    }*/

    if(timeStep == 1.0d){
      rainlevel = rainlevel1
      snowlevel = snowlevel1
    }else if(timeStep == 3.0d){
      rainlevel = rainlevel3
      snowlevel = snowlevel3
    }else if(timeStep == 12.0d){
      rainlevel = rainlevel12
      snowlevel = snowlevel12
    }


    for(i <- 0 until length){
      var Phenomenon: Double = 0
      //晴雨判断
      pre(i) match {
        //1、无雨情况下的按云量判断相态
        case pp if pp == 0 => {
          cld(i) match {
            case cd if cd < 20 => Phenomenon = 0
            case cd if cd > 80 => Phenomenon = 2
            case _ => Phenomenon = 1
          }
        }
        //2、有雨情况下的按温度判断相态
        case pp if pp > 0 =>{
          tem(i) match {
            case tm if tm > 2 => { //根据降雨量级判断天气现象
              pre(i) match {
                case pp if (pp <= rainlevel(0)) => Phenomenon = 7 //小雨
                case pp if (pp > rainlevel(0) & pp <= rainlevel(1)) => Phenomenon = 8 //中雨
                case pp if (pp > rainlevel(1) & pp <= rainlevel(2)) => Phenomenon = 9 //大雨
                case pp if (pp > rainlevel(2) & pp <= rainlevel(3)) => Phenomenon = 10//暴雨
                case pp if (pp > rainlevel(3) & pp <= rainlevel(4)) => Phenomenon = 11//大暴雨
                case _ => Phenomenon = 12//特大暴雨
              }
            }
            case tm if tm < -2 => { //根据降雪量级判断天气现象
              pre(i) match {
                case pp if (pp <= snowlevel(0)) => Phenomenon = 14 //小雪
                case pp if (pp > snowlevel(0) & pp <= snowlevel(1)) => Phenomenon = 15 //中雪
                case pp if (pp > snowlevel(1) & pp <= snowlevel(2)) => Phenomenon = 16 //大雪
                case _ => Phenomenon = 17//大暴雪
              }
            }
            //雨夹雪天气现象不区分量级
            case _ => Phenomenon = 6
          }
        }
      }
      //晴雨判断结束
      data(i) = Phenomenon
    }
    VariableOperation.creatVars4(data,weatherName,"byte",timeName,heightName,latName,lonName,timeDims,heightDims,latDims,lonDims)
  }*/

  def Weather_Wea1(jsonFile: String, PRErdd: RDD[SciDatasetCW], TEMrdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], WEArdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    this.WeatherBy_pre_tem_cld_wea(jsonFile, PRErdd, TEMrdd, CLOUDrdd, WEArdd, timeName, heightName, latName, lonName, timeStep, weatherName)
  }

  private def WeatherBy_pre_tem_cld_wea(jsonFile: String, PRErdd: RDD[SciDatasetCW], TEMrdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], WEArdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    val alls_KV = MatchSciDatasCW.matchByName1(Array(PRErdd, TEMrdd, CLOUDrdd, WEArdd), jsonFile)
    val wea_rdd = alls_KV.map(
      all => {
        //        val PREname=PropertiesUtil.getrElement_rename(ConstantUtil.PRE)(0)
        //        val TEMname=PropertiesUtil.getrElement_rename(ConstantUtil.TEM)(0)
        //        val CLOUDname=PropertiesUtil.getrElement_rename(ConstantUtil.CLOUD)(0)

        val PREname = Constant.PRE
        val TEMname = Constant.TEM
        val CLOUDname = Constant.CLOUD

        val idName = all._1
        val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)
        val idPREname = SplitMatchInfo.oldName2newName(PREname, idName, timeStep)
        val idTEMname = SplitMatchInfo.oldName2newName(TEMname, idName, timeStep)
        val idCLOUDname = SplitMatchInfo.oldName2newName(CLOUDname, idName, timeStep)

        val PRES: SciDatasetCW = all._2.find(p => p.datasetName.equals(idPREname)).get
        val CLOUDS: SciDatasetCW = all._2.find(p => p.datasetName.equals(idCLOUDname)).get
        val TEMS: SciDatasetCW = all._2.find(p => p.datasetName.equals(idTEMname)).get
        val WeaS: SciDatasetCW = all._2.find(p => p.datasetName.equals(weatherName)).get

        this.WeatherBy_pre_tem_cld_wea(PRES, TEMS, CLOUDS, WeaS, timeName, heightName, latName, lonName, timeStep, weatherName)
      })
    wea_rdd
  }

  private def WeatherBy_pre_tem_cld_wea(PREsci: SciDatasetCW, TEMsci: SciDatasetCW, CLOUDsci: SciDatasetCW, WEAsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): SciDatasetCW = {
    val idName_PRE = AttributesOperationCW.getIDName(PREsci)
    val idName_TEM = AttributesOperationCW.getIDName(TEMsci)
    val idName_CLOUD = AttributesOperationCW.getIDName(CLOUDsci)
    val idName_WEA = AttributesOperationCW.getIDName(WEAsci)
    val idName: String = {
      if (idName_PRE.equals(idName_TEM) && idName_PRE.equals(idName_CLOUD) && idName_PRE.equals(idName_WEA)) {
        idName_PRE
      } else {
        val e: Exception = new Exception("idName匹配出错")
        e.printStackTrace()
        "-1"
      }
    }

    val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)
    val PREname = PropertiesUtil.getrElement_rename(Constant.PRE)(0)
    val CLOUDname = PropertiesUtil.getrElement_rename(Constant.CLOUD, timeStep)(0)
    val TEMname = PropertiesUtil.getrElement_rename(Constant.TEM, timeStep)(0)


    //    val PREname=PropertiesUtil.PREname
    //    val TEMname=PropertiesUtil.TEMname
    //    val CLOUDname=PropertiesUtil.CLOUDname

    val PREV: VariableCW = PREsci.apply(PREname)
    val CLOUDV: VariableCW = CLOUDsci.apply(CLOUDname)
    val TEMV: VariableCW = TEMsci.apply(TEMname)
    val WeaV: VariableCW = WEAsci.apply(weatherName)


    val timeV = PREsci.apply(timeName)
    val heightV = PREsci.apply(heightName)
    val latV = PREsci.apply(latName)
    val lonV = PREsci.apply(lonName)


    val weaVar: VariableCW = this.getWea1Variable0(weatherName, timeStep, PREV, TEMV, CLOUDV, WeaV, timeV, heightV, latV, lonV)

    val vars = new ArrayBuffer[VariableCW]()
    vars += timeV += heightV += latV += lonV += weaVar
    //    val varHash=VariableOperationCW.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))

    val preAtt = PREsci.attributes
    val cldAtt = CLOUDsci.attributes
    val temAtt = TEMsci.attributes

    val fcDate = AttributesOperationCW.getfcdate(preAtt)

    var att = new mutable.HashMap[String, String]
    att = AttributesOperationCW.addTrace(att, "WeatherBy_pre_tem_cld_wea")
    att = AttributesOperationCW.updateFcType(att, Constant.WEATHER)
    att = AttributesOperationCW.updateTimeStep(att, timeStep)
    att = AttributesOperationCW.addIDName(att, idName)
    att = AttributesOperationCW.updatefcdate(att, fcDate)
    att = AttributesOperationCW.addVarName(att, (timeName, heightName, latName, lonName))

    new SciDatasetCW(vars, att, AreaName)
  }

  private def getWea1Variable0(weatherName: String, timeStep: Double, PREV: VariableCW, TEMV: VariableCW, CLOUDV: VariableCW, Wea0V: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW): VariableCW = {
    //    val pre=PREV.data
    //    val tem=TEMV.data
    //    val cld=CLOUDV.data
    //    val wea: Array[Double] = Wea0V.data

    val timeName = timeV.name
    val heightName = heightV.name
    val latName = latV.name
    val lonName = lonV.name

    val timeDims = timeV.getLen()
    val heightDims = heightV.getLen()
    val latDims = latV.getLen()
    val lonDims = lonV.getLen()

    val length = timeDims * heightDims * latDims * lonDims
    //    val data = new Array[Double](length)

    val (rainlevel, snowlevel) = PheRule.Phe_level0(timeStep)

    for (i <- 0 until length) {
      //晴雨判断结束
      Wea0V.setdata(i, PheRule.v1(PREV.dataDouble(i), TEMV.dataDouble(i), CLOUDV.dataDouble(i), Wea0V.dataDouble(i).toByte, rainlevel, snowlevel))
    }
    Wea0V
    //    VariableOperation.creatVars4(wea, weatherName, "byte", timeName, heightName, latName, lonName, timeDims, heightDims, latDims, lonDims)
  }

  def Weather_Wea1(PREsci: SciDatasetCW, TEMsci: SciDatasetCW, CLOUDsci: SciDatasetCW, WEAsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, timeStep: Double, weatherName: String): SciDatasetCW = {
    this.WeatherBy_pre_tem_cld_wea(PREsci, TEMsci, CLOUDsci, WEAsci, timeName, heightName, latName, lonName, timeStep, weatherName)
  }

  def Weather_Wea3(jsonFile: String, PRErdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, PPHSci: SciDatasetCW, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    this.WeatherBy_pre_pph_cld_wea(jsonFile, PRErdd, CLOUDrdd, timeName, heightName, latName, lonName, PPHSci, timeStep, weatherName)
  }

  private def WeatherBy_pre_pph_cld_wea(jsonFile: String, PRErdd: RDD[SciDatasetCW], CLOUDrdd: RDD[SciDatasetCW], timeName: String, heightName: String, latName: String, lonName: String, PPHSci: SciDatasetCW, timeStep: Double, weatherName: String): RDD[SciDatasetCW] = {
    val alls_KV = MatchSciDatasCW.matchByName1(Array(PRErdd, CLOUDrdd), jsonFile)
    val wea_rdd = alls_KV.map(
      all => {
        //        val PREname=PropertiesUtil.getrElement_rename(ConstantUtil.PRE)(0)
        //        val PPHname=PropertiesUtil.getrElement_rename(ConstantUtil.PPH)(0)
        //        val CLOUDname=PropertiesUtil.getrElement_rename(ConstantUtil.CLOUD)(0)

        val PREname = Constant.PRE
        val TEMname = Constant.TEM
        val CLOUDname = Constant.CLOUD

        val idName = all._1
        val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)
        val idPREname = SplitMatchInfo.oldName2newName(PREname, idName, timeStep)
        val idCLOUDname = SplitMatchInfo.oldName2newName(CLOUDname, idName, timeStep)

        val PRES: SciDatasetCW = all._2.find(p => p.datasetName.equals(idPREname)).get
        val CLOUDS: SciDatasetCW = all._2.find(p => p.datasetName.equals(idCLOUDname)).get

        this.WeatherBy_pre_pph_cld_wea(PRES, CLOUDS, timeName, heightName, latName, lonName, PPHSci, timeStep, weatherName)
      })
    wea_rdd
  }

  private def WeatherBy_pre_pph_cld_wea(PREsci: SciDatasetCW, CLOUDsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, PPHsci: SciDatasetCW, timeStep: Double, weatherName: String): SciDatasetCW = {
    val idName_PRE = AttributesOperationCW.getIDName(PREsci)
    val idName_PPH = AttributesOperationCW.getIDName(PPHsci)
    val idName_CLOUD = AttributesOperationCW.getIDName(CLOUDsci)
    val idName: String = {
      if (idName_PRE.equals(idName_PPH) && idName_PRE.equals(idName_CLOUD)) {
        idName_PRE
      } else {
        val e: Exception = new Exception("idName匹配出错")
        e.printStackTrace()
        "-1"
      }
    }

    val AreaName = SplitMatchInfo.oldName2newName(weatherName, idName, timeStep)


    val PREname = Constant.PRE
    val CLOUDname = Constant.CLOUD
    val PPHname = Constant.PPH


    val PREV: VariableCW = PREsci.apply(PREname)
    val CLOUDV: VariableCW = CLOUDsci.apply(CLOUDname)
    val PPHV: VariableCW = PPHsci.apply(PPHname)

    val timeV = PREsci.apply(timeName)
    val heightV = PREsci.apply(heightName)
    val latV = PREsci.apply(latName)
    val lonV = PREsci.apply(lonName)

    val (timeName_pph, latName_pph, lonName_pph) = AttributesOperationCW.getVarName_tlatlon(PPHsci)

    val timeV_pph = PPHsci.apply(timeName_pph)
    val latV_pph = PPHsci.apply(latName_pph)
    val lonV_pph = PPHsci.apply(lonName_pph)


    val weaVar: VariableCW = this.getWea3Variable0(weatherName, timeStep, PREV, CLOUDV, timeV, heightV, latV, lonV, PPHV, timeV_pph, latV_pph, lonV_pph)

    val vars = new ArrayBuffer[VariableCW]()
    vars += timeV += heightV += latV += lonV += weaVar
    //    val varHash = VariableOperationCW.allVars2NameVar4(this.getClass.toString, idName, vars, (timeName, heightName, latName, lonName))

    val preAtt = PREsci.attributes
    val cldAtt = CLOUDsci.attributes
    val pphAtt = PPHsci.attributes

    val fcDate = AttributesOperationCW.getfcdate(preAtt)

    var att = new mutable.HashMap[String, String]
    att = AttributesOperationCW.addTrace(att, "WeatherBy_pre_tem_cld_wea")
    att = AttributesOperationCW.updateFcType(att, Constant.WEATHER)
    att = AttributesOperationCW.updateTimeStep(att, timeStep)
    att = AttributesOperationCW.addIDName(att, idName)
    att = AttributesOperationCW.updatefcdate(att, fcDate)
    att = AttributesOperationCW.addVarName(att, (timeName, heightName, latName, lonName))

    new SciDatasetCW(vars, att, AreaName)
  }

  private def getWea3Variable0(weatherName: String, timeStep: Double, PREV: VariableCW, CLOUDV: VariableCW, timeV: VariableCW, heightV: VariableCW, latV: VariableCW, lonV: VariableCW, PPHV: VariableCW, timeV_pph: VariableCW, latV_pph: VariableCW, lonV_pph: VariableCW): VariableCW = {
    //    val pre = PREV.data
    //    val pph = PPHV.data
    //    val cld = CLOUDV.data


    val time = timeV.dataDouble
    val height = heightV.dataDouble
    val lat = latV.dataDouble
    val lon = lonV.dataDouble

    val timeLen = timeV.getLen()
    val heightLen = heightV.getLen()
    val latLen = latV.getLen()
    val lonLen = lonV.getLen()

    val shape = Array(timeLen, heightLen, latLen, lonLen)
    val WEA = ucar.ma2.Array.factory(DataType.getType(this.weaDataType), shape)

    val time_pph = timeV_pph.dataDouble()
    val lat_pph = latV_pph.dataDouble()
    val lon_pph = lonV_pph.dataDouble()

    val timeName = timeV.name
    val heightName = heightV.name
    val latName = latV.name
    val lonName = lonV.name


    //    val timeLen_pph=time_pph.length
    val latLen_pph = latV_pph.getLen()
    val lonLen_pph = lonV_pph.getLen()

    val length = timeLen * heightLen * latLen * lonLen

    val (rainlevel, snowlevel) = PheRule.Phe_level0(timeStep)

    for (i <- 0 until timeLen - 1) {
      val timeValue = timeV.dataDouble(i)
      val I = ArrayOperation.nearIndex_sequence(time_pph, timeValue)
      for (j <- 0 until heightLen - 1) {
        val heightValue = height(j)
        val J = 0
        for (k <- 0 until latLen - 1) {
          val latValue = lat(k)
          val K = ArrayOperation.nearIndex_sequence(lat_pph, latValue)
          for (l <- 0 until lonLen - 1) {
            val lonValue = lon(l)
            val L = ArrayOperation.nearIndex_sequence(lon_pph, lonValue)
            val index0 = IndexOperation.getIndex4_thlatlon(i, j, k, l, heightLen, latLen, lonLen)
            val index1 = IndexOperation.getIndex3_tlatlon(I, K, L, latLen_pph, lonLen_pph)
            val pphValue = PPHV.dataDouble(index1)
            val preValue = PREV.dataDouble(index0)
            val cldValue = CLOUDV.dataDouble(index0)
            ucarma2ArrayOperationCW.setObject(WEA, index0, PheRule.v3(preValue, pphValue, cldValue, rainlevel, snowlevel))
          }
        }
      }
    }
    VariableOperationCW.creatVars4(WEA, weatherName, timeName, heightName, latName, lonName)
    //    VariableOperation.creatVars4(pre, weatherName, "byte", timeName, heightName, latName, lonName, timeLen, heightLen, latLen, lonLen)
  }

  def Weather_Wea3(PREsci: SciDatasetCW, CLOUDsci: SciDatasetCW, timeName: String, heightName: String, latName: String, lonName: String, PPHsci: SciDatasetCW, timeStep: Double, weatherName: String): SciDatasetCW = {
    this.WeatherBy_pre_pph_cld_wea(PREsci, CLOUDsci, timeName, heightName, latName, lonName, PPHsci, timeStep, weatherName)
  }

  /*private def WeatherBy_pre_pph_cld0_1h(jsonFile:String,PRErdd:RDD[SciDataset],PPHsci:SciDataset,CLOUDrdd:RDD[SciDataset],timeName:String,heightName:String,latName:String,lonName:String,timeStep:Double,weatherName:String): RDD[SciDataset] ={
    val alls_KV=MatchSciDatas.matchByName1(Array(PRErdd,CLOUDrdd),jsonFile)
    val wea_rdd=alls_KV.map(
      all => {
        val PREname=PropertiesUtil.getrElement_rename(ConstantUtil.PRE)(0)
        val CLOUDname=PropertiesUtil.getrElement_rename(ConstantUtil.CLOUD)(0)

        val idName=all._1
        val AreaName=SplitMatchInfo.oldName2newName(weatherName,idName,timeStep)
        val idPREname=SplitMatchInfo.oldName2newName(PREname,idName,timeStep)
        val idCLOUDname=SplitMatchInfo.oldName2newName(CLOUDname,idName,timeStep)

        val PRES:SciDataset=all._2.find(p=>p.datasetName.equals(idPREname)).get
        val CLOUDS:SciDataset=all._2.find(p=>p.datasetName.equals(idCLOUDname)).get

        this.WeatherBy_pre_pph_cld0_1h(PRES,PPHsci,CLOUDS,timeName,heightName,latName,lonName,timeStep,weatherName)
      })
    wea_rdd
  }
  private def WeatherBy_pre_pph_cld0_1h(PREsci:SciDataset, PPHsci:SciDataset, CLOUDsci:SciDataset, timeName:String, heightName:String, latName:String, lonName:String, timeStep:Double, weatherName:String): SciDataset ={
    val idName_PRE=AttributesOperation.getIDName(PREsci)
    val idName_CLOUD=AttributesOperation.getIDName(CLOUDsci)
    val idName:String={
      if(idName_PRE.equals(idName_CLOUD)){
        idName_PRE
      }else{
        val e:Exception=new Exception("idName匹配出错")
        e.printStackTrace()
        "-1"
      }
    }

    val AreaName=SplitMatchInfo.oldName2newName(weatherName,idName,timeStep)
    val PREname=PropertiesUtil.getrElement_rename(ConstantUtil.PRE)(0)
    val CLOUDname=PropertiesUtil.getrElement_rename(ConstantUtil.CLOUD,timeStep)(0)


    val PREV:Variable=PREsci.apply(PREname)
    val CLOUDV:Variable=CLOUDsci.apply(CLOUDname)
    val (pphV:Variable,pphVars:(Variable,Variable,Variable))={
      val PPHname:String=AttributesOperation.getDataNames(PPHsci).head
      val varsName:(String,String,String,String)=AttributesOperation.getVarName(PPHsci)
      (PPHsci.apply(PPHname),(PPHsci.apply(varsName._1),PPHsci.apply(varsName._3),PPHsci.apply(varsName._4)))
    }

    val timeV=PREsci.apply(timeName)
    val heightV=PREsci.apply(heightName)
    val latV=PREsci.apply(latName)
    val lonV=PREsci.apply(lonName)


    val weaVar:Variable=this.getWea2Variable0(weatherName,timeStep,PREV,CLOUDV,(timeV,heightV,latV,lonV),pphV,pphVars)

    val vars=new ArrayBuffer[Variable]()
    vars+=timeV+=heightV+=latV+=lonV+=weaVar
    val varHash=VariableOperation.allVars2NameVar4(this.getClass.toString,idName,vars,(timeName,heightName,latName,lonName))

    val preAtt=PREsci.attributes
    val cldAtt=CLOUDsci.attributes

    val fcDate=AttributesOperation.getfcdate(preAtt)

    var att=new mutable.HashMap[String,String]
    att=AttributesOperation.addTrace(att,"WeatherBy_pre_pph_cld0")
    att=AttributesOperation.updateFcType(att,ConstantUtil.WEATHER)
    att=AttributesOperation.updateTimeStep(att,timeStep)
    att=AttributesOperation.addIDName(att,idName)
    att=AttributesOperation.updatefcdate(att,fcDate)
    att=AttributesOperation.addVarName(att,(timeName,heightName,latName,lonName))

    new SciDataset(varHash, att, AreaName)
  }
  private def getWea2Variable0(weatherName:String,timeStep:Double,PREV:Variable,CLOUDV:Variable,splitVs:(Variable,Variable,Variable,Variable),PPHV:Variable,unSplitVs:(Variable,Variable,Variable)): Variable ={
    val pre=PREV.data
    val pph=PPHV.data
    val cld=CLOUDV.data

    val (timeV:Variable,heightV:Variable,latV:Variable,lonV:Variable)=splitVs
    val (pphtimeV:Variable,pphlatV:Variable,pphlonV:Variable)=unSplitVs

    val timeName=timeV.name
    val heightName=heightV.name
    val latName=latV.name
    val lonName=lonV.name



    val time:Array[Double]=timeV.data
    val height:Array[Double]=heightV.data
    val lat:Array[Double]=latV.data
    val lon:Array[Double]=lonV.data

    val pphtime:Array[Double]=pphtimeV.data
    val pphlat:Array[Double]=pphlatV.data
    val pphlon:Array[Double]=pphlonV.data

    val timeLen =time.length
    val heightLen =height.length
    val latLen =lat.length
    val lonLen =lon.length

    val pphtimeLen=pphtime.length
    val pphlatLen=pphlat.length
    val pphlonLen=pphlon.length

    val length = timeLen * heightLen * latLen * lonLen
    val data = new Array[Double](length)

    val (rainlevel,snowlevel)=PheRule.Phe_level0(timeStep)

    for(j<- 0 to heightLen-1){
      for(i<- 0 to timeLen-1){
        val time0=time(i)
        val pphtimeIndex=ArrayOperation.nearIndex_sequence(pphtime,time0)
        for(k<- 0 to latLen-1){
          val lat0=lat(k)
          val pphlatIndex=ArrayOperation.nearIndex_sequence(pphlat,lat0)
          for(l<- 0 to lonLen-1){
            val lon0=lon(l)
            val pphlonIndex=ArrayOperation.nearIndex_sequence(pphlon,lon0)
            val Index0=VariableOperation.getIndex4_thlatlon(i,j,k,l,heightLen,latLen,lonLen)
            val Index1=VariableOperation.getIndex3_tlatlon(pphtimeIndex,pphlatIndex,pphlonIndex,pphlatLen,pphlonLen)
            data(Index0)=PheRule.v3(pre(Index0),pph(Index1),cld(Index0),rainlevel,snowlevel)
          }
        }
      }
    }


    VariableOperation.creatVars4(data,weatherName,"byte",timeName,heightName,latName,lonName,timeLen,heightLen,latLen,lonLen)
  }
*/
  def main(args: Array[String]): Unit = {
    /*val fcdate=DataFormatUtil.YYYYMMDDHHMMSS0("20180308080000")
    //"D:\\Data\\forecast\\Input\\Real\\20180308\\08\\PPH\\2018030808_PPH.nc"
    val pphfile=PropertiesUtil.getgr2bFile("PPH",fcdate)
    var pphSci=ReadFile.ReadNcScidata(pphfile)
    pphSci=AttributesOperation.addVarName(pphSci,("time","","lat","lon"))
    pphSci=AttributesOperation.addDataNames(pphSci,Array("Precipitation_type_surface_3_Hour_Accumulation"))
    val timeStep=1.0d

    val preRdd=ReadFile.ReadNcRdd(PropertiesUtil.getfcPath(timeStep,ConstantUtil.PRE,fcdate))
    val cloudRdd=ReadFile.ReadNcRdd(PropertiesUtil.getfcPath(timeStep,ConstantUtil.CLOUD,fcdate))
    val (timeName,heightName,latName,lonName)=PropertiesUtil.getVarName()
    val weaRdd=this.WeatherBy_pre_pph_cld0_1h(preRdd,pphSci,cloudRdd,timeName,heightName,latName,lonName,timeStep,ConstantUtil.WEATHER)

    val weaFile=PropertiesUtil.getfcPath(timeStep,ConstantUtil.WEATHER,fcdate)
    WriteFile.WriteNcFile_test(weaRdd,weaFile)*/
  }
}
