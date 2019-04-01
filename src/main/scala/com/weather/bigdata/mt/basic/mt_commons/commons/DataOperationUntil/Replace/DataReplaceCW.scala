package com.weather.bigdata.mt.basic.mt_commons.commons.DataOperationUntil.Replace

import com.weather.bigdata.it.nc_grib.ReadWrite.{AttributesConstant, AttributesOperationCW}
import com.weather.bigdata.it.nc_grib.core.{SciDatasetCW, VariableCW}
import com.weather.bigdata.it.nc_grib.utils.IndexOperation
import com.weather.bigdata.it.utils.operation.{ArrayOperation, NumOperation}
import org.apache.spark.rdd.RDD

object DataReplaceCW {

  //firstData cover secondData
  /**
    *
    * @param matchRDD
    * @param timeName    (firsttimeName,secondtimeName)
    * @param heightName  (firstheightName,secondheightName)
    * @param latName     (firstlatName,secondlatName)
    * @param lonName     (firstlonName,secondlonName)
    * @param ArrDataName (firstdataName,seconddataName)
    * @return (first data cover second data,return the second revised data.)
    */
  def dataReplace (matchRDD: RDD[(SciDatasetCW, SciDatasetCW)], timeName: String, heightName: String, latName: String, lonName: String, ArrDataName: Array[String]): RDD[SciDatasetCW] = {
    matchRDD.map(
      matchScidata => this.dataReplace(matchScidata, timeName, heightName, latName, lonName, ArrDataName)
    )
  }

  def dataReplace (matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, heightName: String, latName: String, lonName: String, ArrDataName: Array[String]): SciDatasetCW = {
    this.dataReplace2(matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, heightName: String, latName: String, lonName: String, ArrDataName: Array[String])
  }

  //对Variable操作
  private def dataReplace2 (matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, heightName: String, latName: String, lonName: String, dataNames: Array[String]): SciDatasetCW = {
    val scidata0 = matchScidata._1
    val datasetName = scidata0.datasetName
    var scidata1 = matchScidata._2

    scidata0.attributes.foreach(f => {
      if (!f._1.equals(AttributesConstant.idNameKey)) {
        scidata1.update(f._1, f._2)
      }
    })


    val (time0: Array[Double], height0: Array[Double], lat0: Array[Double], lon0: Array[Double]) = {
      (NumOperation.K3dec(scidata0.apply(timeName).dataDouble( )), NumOperation.K3dec(scidata0.apply(heightName).dataDouble), NumOperation.K3dec(scidata0.apply(latName).dataDouble), NumOperation.K3dec(scidata0.apply(lonName).dataDouble))
    }
    val (time1: Array[Double], height1: Array[Double], lat1: Array[Double], lon1: Array[Double]) = {
      (NumOperation.K3dec(scidata1.apply(timeName).dataDouble), NumOperation.K3dec(scidata1.apply(heightName).dataDouble), NumOperation.K3dec(scidata1.apply(latName).dataDouble), NumOperation.K3dec(scidata1.apply(lonName).dataDouble))
    }
    val (time0Len, height0Len, lat0Len, lon0Len) = {
      (time0.length, height0.length, lat0.length, lon0.length)
    }
    val (time1Len, height1Len, lat1Len, lon1Len) = {
      (time1.length, height1.length, lat1.length, lon1.length)
    }

    dataNames.foreach(dataName => {
      val data0V = scidata0.apply(dataName)

      val data1V = scidata1.apply(dataName)


      val timeIndexs: Array[Int] = {
        val time00 = {
          if (time0Len <= time1Len) {
            time0
          } else {
            time0.filter(p => time1.contains(p))
          }
        }
        ArrayOperation.HashMapSearch(time1, time00)
      }
      val heightIndexs: Array[Int] = {
        if (height0Len == 1 && height1Len == 1) {
          println("height0Len==1 && height1Len==1,取heightIndexs==0")
          Array(0)
        } else {
          ArrayOperation.HashMapSearch(height1, height0)
        }
      }
      val latIndexs: Array[Int] = ArrayOperation.HashMapSearch(lat1, lat0)
      val lonIndexs: Array[Int] = ArrayOperation.HashMapSearch(lon1, lon0)


      for (i <- 0 to timeIndexs.length - 1) {
        val timeIndex = timeIndexs(i)
        for (j <- 0 to height0.length - 1) {
          val heightIndex = heightIndexs(j)
          for (k <- 0 to lat0.length - 1) {
            val latIndex = latIndexs(k)
            for (l <- 0 to lon0.length - 1) {
              val lonIndex = lonIndexs(l)

              val Index0 = IndexOperation.getIndex4_thlatlon(i, j, k, l, height0Len, lat0Len, lon0Len)
              val Index1 = IndexOperation.getIndex4_thlatlon(timeIndex, heightIndex, latIndex, lonIndex, height1Len, lat1Len, lon1Len)

              data1V.setdata(Index1, data0V.dataDouble(Index0))
            }
          }
        }
      }


    })


    scidata1 = AttributesOperationCW.addTrace(scidata1, "DataCoverBy:" + datasetName)
    scidata1
  }

  //reduceLatLon专属
  def dataReplace1 (matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, latName: String, lonName: String, ArrDataName: Array[String]): SciDatasetCW = {
    this.data3Replace0(matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, latName: String, lonName: String, ArrDataName: Array[String])
  }

  //reduceLatLon专属
  private def data3Replace0 (matchScidata: (SciDatasetCW, SciDatasetCW), timeName: String, latName: String, lonName: String, dataNames: Array[String]): SciDatasetCW = {
    val scidata0 = matchScidata._1
    val datasetName = scidata0.datasetName
    var scidata1 = matchScidata._2

    //作用?
    scidata0.attributes.foreach(f => {
      if (!f._1.equals(AttributesConstant.idNameKey)) {
        scidata1.update(f._1, f._2)
      }
    })


    /*val (time0:Array[Double],height0:Array[Double],lat0:Array[Double],lon0:Array[Double])={
      (NumOperation.K3dec(scidata0.apply(timeName).dataDouble()),NumOperation.K3dec(scidata0.apply(heightName).dataDouble),NumOperation.K3dec(scidata0.apply(latName).dataDouble),NumOperation.K3dec(scidata0.apply(lonName).dataDouble))
    }

    val (time1:Array[Double],height1:Array[Double],lat1:Array[Double],lon1:Array[Double])={
      (NumOperation.K3dec(scidata1.apply(timeName).dataDouble),NumOperation.K3dec(scidata1.apply(heightName).dataDouble),NumOperation.K3dec(scidata1.apply(latName).dataDouble),NumOperation.K3dec(scidata1.apply(lonName).dataDouble))
    }
    val (time0Len,height0Len,lat0Len,lon0Len)={
      (time0.length,height0.length,lat0.length,lon0.length)
    }
    val (time1Len,height1Len,lat1Len,lon1Len)={
      (time1.length,height1.length,lat1.length,lon1.length)
    }*/


    val timeIndexs: Array[Int] = {
      val timeV0: VariableCW = scidata0.apply(timeName)
      val timeV1: VariableCW = scidata1.apply(timeName)
      val time0Len: Int = timeV0.getLen( )
      val time1Len: Int = timeV1.getLen( )
      val time0 = NumOperation.K3dec(timeV0.dataDouble( ))
      val time1 = NumOperation.K3dec(timeV1.dataDouble( ))
      //      val time0=timeV0.dataDouble()
      //      val time1=timeV1.dataDouble()

      //      val time00={
      //        if(time0Len<=time1Len){
      //          time0
      //        }else{
      //          time0.filter(p=>time1.contains(p))
      //        }
      //      }
      val timeIndexs0 = ArrayOperation.HashMapSearch(time0, time1)
      //      val timeIndexs0=ArrayOperation.nearIndexs_sequence(time1,time00)
      timeIndexs0
    }

    val (latIndexs: Array[Int], lat0Len: Int, lat1Len: Int) = {
      val latV0: VariableCW = scidata0.apply(latName)
      val latV1: VariableCW = scidata1.apply(latName)
      val lat0Len0 = latV0.getLen( )
      val lat1Len0 = latV1.getLen( )
      val lat0 = NumOperation.K3dec(latV0.dataDouble( ))
      val lat1 = NumOperation.K3dec(latV1.dataDouble( ))
      val latIndexs0: Array[Int] = ArrayOperation.HashMapSearch(lat1, lat0)
      (latIndexs0, lat0Len0, lat1Len0)
    }

    val (lonIndexs: Array[Int], lon0Len: Int, lon1Len: Int) = {
      val lonV0: VariableCW = scidata0.apply(lonName)
      val lonV1: VariableCW = scidata1.apply(lonName)
      val lon0Len0 = lonV0.getLen( )
      val lon1Len0 = lonV1.getLen( )
      val lon0 = NumOperation.K3dec(lonV0.dataDouble( ))
      val lon1 = NumOperation.K3dec(lonV1.dataDouble( ))
      val lonIndexs0 = ArrayOperation.HashMapSearch(lon1, lon0)
      (lonIndexs0, lon0Len0, lon1Len0)
    }

    dataNames.foreach(dataName => {
      val data0V = scidata0.apply(dataName)

      val data1V = scidata1.apply(dataName)



      for (i <- 0 to timeIndexs.length - 1) {
        val timeIndex = timeIndexs(i)
        for (k <- 0 to lat0Len - 1) {
          val latIndex = latIndexs(k)
          for (l <- 0 to lon0Len - 1) {
            val lonIndex = lonIndexs(l)

            //与以上搜索对应
            val Index0 = IndexOperation.getIndex4_thlatlon(timeIndex, 0, k, l, 1, lat0Len, lon0Len)
            val Index1 = IndexOperation.getIndex3_tlatlon(i, latIndex, lonIndex, lat1Len, lon1Len)

            data1V.setdata(Index1, data0V.dataDouble(Index0))
          }
        }

      }


    })


    scidata1 = AttributesOperationCW.addTrace(scidata1, "DataCoverBy:" + datasetName)
    scidata1
  }

  def main (args: Array[String]): Unit = {

  }
}
