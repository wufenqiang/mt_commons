package com.weather.bigdata.mt.basic.mt_commons.commons.KVStoreUtil

import java.io.{BufferedReader, InputStreamReader}
import java.net.{URL, URLConnection}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation, SetOperation}
import redis.clients.jedis.Jedis

import scala.collection.mutable

object KVStoreConf {
  val hostNameKey="host_name"
  val portKey="port"
  val authKey="password"

  val ChangeRefKey="_RF"

  private def redisConf(metaurl:String): JSONObject ={
    val paramKey:String="ALLTYPE"
    val metauri:URL = new URL(metaurl + "?param="+paramKey)
    val conn : URLConnection= metauri.openConnection
    val reader :BufferedReader= new BufferedReader(new InputStreamReader(conn.getInputStream))
    val sb = new StringBuilder
    var c :String = reader.readLine
    while (c!= null) {
      //      println(c)
      sb.append(c)
      c = reader.readLine
    }
    reader.close()
    val configJSON : JSONObject= JSON.parseObject(sb.toString).getJSONObject("result")
    configJSON
  }
  private def redisConf(metaurl:String,`type`:String): JSONObject ={
    val allType:JSONObject=this.redisConf(metaurl)
    allType.getJSONObject(`type`)
  }
  def gethostJson(metaurl:String,`type`:String):JSONObject={
    val configJSON:JSONObject=this.redisConf(metaurl,`type`)
    val hostJSON:JSONObject=configJSON.getJSONObject("CONN")
    hostJSON
  }
  def showAllTypesIP(metaurl:String,timeout:Int=30000): Unit ={
    val allTypeJSON:JSONObject=this.redisConf(metaurl)
    val allTypes:mutable.HashSet[String]=SetOperation.Set2scalaSet(allTypeJSON.keySet())
    allTypes.foreach(`type`=>{
      val hosts:Array[(String,String,Int,String)]=this.gethost_port(metaurl,`type`)
      hosts.foreach(f=>{
        val hostName=f._1
        val ip=f._2
        val port=f._3
        val auth=f._4

        val jedis = new Jedis(ip, port, timeout)
        jedis.auth(auth)
        val size:Long=jedis.dbSize()
        jedis.close()

//        println(f._1+"="+f._2+":"+f._3+"("+f._4+")")
        println(hostName+"="+ip+":"+port+";dbSize="+size)
      })
    })
  }
  def getKeySize(metaurl:String,`type`:String,timeout:Int=30000): Long ={
    val sizes=this.getKeySizes(metaurl,`type`,timeout:Int).map(f=>f._2)
    sizes.reduce((x,y)=>(x+y))
  }
  def getKeySizes(metaurl:String,`type`:String,timeout:Int=30000): Array[(String,Long)] ={
    val hosts:Array[(String,String,Int,String)]=this.gethost_port(metaurl,`type`)
    hosts.map(f=>{
      val hostName=f._2
      val port=f._3
      val auth=f._4
      val jedis = new Jedis(hostName, port, timeout)
      jedis.auth(auth)
      val size:Long=jedis.dbSize()
      jedis.close()
      (f._1,size)
    })
  }
  private def getTypes(metaurl:String): Array[String] ={
    val allTypeJSON:JSONObject=this.redisConf(metaurl)
    val allTypes:mutable.HashSet[String]=SetOperation.Set2scalaSet(allTypeJSON.keySet())
    allTypes.toArray
  }
  def showSpeed (metaurl:String,`type`:String):Unit={
    val Total:Long=18493601

    val now0:Long=System.currentTimeMillis()
    val keysize0:Long=KVStoreConf.getKeySize(metaurl,`type`)

    Thread.sleep(1000)

    val now1:Long=System.currentTimeMillis()
    val keysize1:Long=KVStoreConf.getKeySize(metaurl,`type`)

    //s
    val dtime:Double=(now1-now0).toDouble
    val dkey:Long=(keysize1-keysize0)*1000

    val speed:Double=dkey.toDouble/dtime

    val pre:Double=keysize1.toDouble/Total.toDouble
    println(`type`+":size="+keysize1+"("+pre+"),speed="+speed+"/s")
  }
  def showSpeeds(metaurl:String,`type`:String): Unit ={
//    val Total:Long=18493601

    val now0:Long=System.currentTimeMillis()
    val keysize0:Array[(String,Long)]=KVStoreConf.getKeySizes(metaurl,`type`)

    Thread.sleep(1000)

    val now1:Long=System.currentTimeMillis()
    val keysize1:Array[(String,Long)]=KVStoreConf.getKeySizes(metaurl,`type`)

    //s
    val dtime:Double=(now1-now0).toDouble
    val dkeys:Array[Double]=ArrayOperation.eachMinus(keysize1.map(f=>f._2),keysize0.map(f=>f._2)).map(f=>f.toDouble*1000/dtime)

    print(`type`+":size=")
    keysize1.foreach(keysize11=>{
      print(keysize1+";")
    })
    print(";speed=")
    dkeys.foreach(dkey=>{
      print(dkey+"/s;")
    })
    println()

  }
  def gethost_port(metaurl:String,`type`:String):Array[(String,String,Int,String)]={
    val hostJson:JSONObject=this.gethostJson(metaurl,`type`)
    val dbSet:mutable.HashSet[String]=SetOperation.Set2scalaSet(hostJson.keySet())
    //    println()
    val hosts:Array[(String,String,Int,String)]={
      try{
        dbSet.map(dbNo=>{
          val hostJsonNo=hostJson.getJSONObject(dbNo)
          (`type`+"_"+dbNo,hostJsonNo.getString("host_name"),hostJsonNo.getIntValue("port"),hostJsonNo.getString("password"))
        }).toArray
      }catch{
        case e:Exception=>{
          e.printStackTrace()
          Array(("","",99999,""))
        }
      }
    }
    hosts
  }

  def latIndex2lat(latIndex:Int): Double ={
    NumOperation.K2dec(latIndex * 0.01d)
  }
  def lonIndex2lon(lonIndex:Int): Double ={
    NumOperation.K2dec(lonIndex * 0.01d + 70.0d)
  }
  def type2timeStep(`type`:String): Double ={
    val type0=`type`.toLowerCase
    if("n1h".equals(type0)){
      1.0d
    }else if("n12h".equals(type0)){
      12.0d
    }else{
      val msg="type="+`type`+"未配置type2timeStep,输出默认值-1"
      val e:Exception=new Exception(msg)
      e.printStackTrace()
      -1
    }
  }
  def main(args:Array[String]): Unit ={
    val latIndex:Int=1547
    val lonIndex:Int=1352
    val lat=this.latIndex2lat(latIndex)
    val lon=this.lonIndex2lon(lonIndex)
    println("lat="+lat+";lon="+lon)
  }
}
