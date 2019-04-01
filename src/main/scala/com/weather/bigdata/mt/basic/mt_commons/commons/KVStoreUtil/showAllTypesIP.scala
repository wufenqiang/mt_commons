package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

object showAllTypesIP {
  def main(args:Array[String]): Unit ={
    val metaurl:String={

      "http://220.243.129.242:6788/weatherstore/v1/api/metaapi"

    }
    KVStoreConf.showAllTypesIP(metaurl)
  }
}
