package com.weather.bigdata.mt.basic.mt_commons.commons.ReviseUtil

import com.weather.bigdata.mt.basic.mt_commons.commons.Constant
import com.weather.bigdata.mt.basic.mt_commons.commons.PheUtil.PheRule

private object OcfRule {
  def ocf2(fcType:String,fcvalue:Double,ocfvalue:Double): Double ={
    if (fcType.equals(Constant.WEATHER)) {
      PheRule.v2(fcvalue.toByte, ocfvalue.toByte).toDouble
    }else{
      ocfvalue
    }
  }

  def ocf2(fcType: String, fcvalue: Float, ocfvalue: Float): Float = {
    if (fcType.equals(Constant.WEATHER)) {
      PheRule.v2(fcvalue.toByte, ocfvalue.toByte).toFloat
    } else {
      ocfvalue
    }
  }
}
