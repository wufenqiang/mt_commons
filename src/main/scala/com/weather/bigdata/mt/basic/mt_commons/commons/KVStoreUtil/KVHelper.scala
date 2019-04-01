package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import com.weather.bussiness.platform.weatherstore.kvstore.{KVStoreMultiDBHelper, KVStoreSimpleHelper}

object KVHelper {
  def KVHelper(metaurl:String,types:String,timeout:Int) ={
//    new KVStoreSimpleHelper(metaurl,types,timeout)
    new KVStoreMultiDBHelper(metaurl,types,timeout)
  }
}
