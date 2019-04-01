package com.weather.bigdata.mt.basic.mt_commons.commons.QualityControlUtil

import com.weather.bigdata.mt.basic.mt_commons.commons.Constant


private object LimitData {
  private val TEM_MAX: Double = 60.0d
  private val TEM_MIN: Double = -60.0d
  private val dTEMLimit: Double = 8.0d

  private val CLOUD_MAX: Double = -1d
  private val CLOUD_MIN: Double = -1d

  private val PRE_MAX: Double = -1d
  private val PRE_MIN: Double = -1d

  private val RHU_MAX: Double = -1d
  private val RHU_MIN: Double = -1d

  private val WINU_MAX: Double = -1d
  private val WINU_MIN: Double = -1d

  private val WINV_MAX: Double = -1d
  private val WINV_MIN: Double = -1d

  private val PRS_MAX: Double = -1d
  private val PRS_MIN: Double = -1d
  /**
    * 雨雪相态
    */
  private val Set_PPH : Set[Byte]= Set(0,1,2,3,4)
  /**
    * 沙尘
    */
  private val Set_DUST : Set[Byte] = Set(0,1,2,3,4)
  /**
    * 霾
    */
  private val Set_HAZE : Set[Byte] = Set(0,1,2,3,4)
  /**
    * 雾
    */
  private val Set_FOG : Set[Byte] = Set(0,1,2,3,4)
  /**
    * 天气现象编码
    */
  private val Set_WEATHER :Set[Byte] =Set(0, 1, 2, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 29, 53)
  /**
    * 风速等级
    */
  private val Set_WINS :Set[Byte]  = Set(1, 2, 3, 4, 5, 6, 7, 8, 9)
  /**
    * 风向等级
    */
  private val Set_WIND:Set[Byte]= Set(0, 1, 2, 3, 4, 5, 6, 7, 8)

  def getLimit(FcType:String): (Double,Double) ={
    var Min:Double= 999.0d
    var Max:Double= -999.0d
    FcType match{
      case "TEM_VarName" =>{
        Min=this.TEM_MIN
        Max=this.TEM_MAX
      }
      case "CLOUD_VarName" =>{
        Min=this.CLOUD_MIN
        Max=this.CLOUD_MAX
      }
      case "PRE_VarName" =>{
        Min=this.PRE_MIN
        Max=this.PRE_MAX
      }
      case "RHU_VarName" =>{
        Min=this.RHU_MIN
        Max=this.RHU_MAX
      }
      case "WINU"=>{
        Min=this.WINU_MIN
        Max=this.WINU_MAX
      }
      case "WINV"=>{
        Min=this.WINV_MIN
        Max=this.WINV_MAX
      }
      case _=>()
    }
    (Min,Max)
  }
  def getLimitSet(FcType:String): Set[Byte]={
    var set:Set[Byte]=Set()
    FcType match{
      case "WIND_VarName" =>{
        set=this.Set_WIND
      }
      case "WINS"=>{
        set=this.Set_WINS
      }
      case "PPH"=>{
        set=this.Set_PPH
      }
      case "DUST"=>{
        set=this.Set_HAZE
      }
      case "HAZE"=>{
        set=this.Set_HAZE
      }
      case "FOG"=>{
        set=this.Set_FOG
      }
      case Constant.WEATHER => {
        set=this.Set_WEATHER
      }
      case _=>()
    }
    set
  }



}
