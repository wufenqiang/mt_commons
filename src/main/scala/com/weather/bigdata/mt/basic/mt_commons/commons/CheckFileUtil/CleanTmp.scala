package com.weather.bigdata.mt.basic.mt_commons.commons.CheckFileUtil

object CleanTmp {
  def main(args:Array[String]): Unit ={
    /*val jsonFile=PropertiesUtil.getjsonFile()
    val jsonRdd=PropertiesUtil.getJsonRdd(jsonFile)
    //    val jsonRdd=PropertiesUtil.getJsonRdd
    /* jsonRdd.foreach(
       jso=>{
         val idName=jso.getString(AttributesOperation.idNameKey)
         //        println("idName="+idName)
         val tmpName=PropertiesUtil.getWriteTmp()
         //        LinuxOperation.getCMDStr("ls -a "+tmpName).toString
         LinuxOperation.getCMDStr("rm "+tmpName+"*.nc").toString
       }
     )*/

    val logs:Array[String]=jsonRdd.map(
      jso=>{
        val idName=jso.getString(AttributesOperation.idNameKey)
        //        println("idName="+idName)
        val tmpName=PropertiesUtil.getWriteTmp()
        LinuxOperation.getCMDStr("ls -a "+tmpName).toString
      }
    ).collect
    HDFSReadWriteUtil.writeHDFSfile("hdfs://dataflow-node-1:9000/data/dataSource/fc/grid/WEATHER/source/test/contents.txt",logs,false)*/
  }
}
