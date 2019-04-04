/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.weather.bigdata.mt.basic.mt_commons.sparkUtil

import java.net.URI

import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import org.dia.core.WWLLN

//提示类
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._
import org.dia.loaders.NetCDFReader._
import org.dia.utils.{NetCDFUtils, WWLLNUtils}

import scala.collection.mutable


class SciSparkContextCW (@transient val sparkContext: SparkContext) extends Serializable {
  /**
    * Log4j Setup
    * By default the natty Parser log4j messages are turned OFF.
    */
  // scalastyle:off classforname
  val DateParserClass = Class.forName("com.joestelmach.natty.Parser")
  // scalastyle:on classforname
  val ParserLevel = org.apache.log4j.Level.OFF
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val defaultPartitions = sparkContext.defaultMinPartitions
  var HTTPCredentials: mutable.Seq[(String, String, String)] = mutable.Seq( )
  LogManager.getLogger(DateParserClass).setLevel(ParserLevel)

  /**
    * SparkContext setup
    * The default matrix library is Scala Breeze
    */
  sparkContext.setLocalProperty(ARRAY_LIB, BREEZE_LIB)


  /**
    * We use kryo serialization.
    * As of nd4j 0.5.0 we need to add the Nd4jRegistrar to avoid nd4j serialization errors.
    * These errors come in the form of NullPointerErrors.
    *
    * @param conf Configuration for a spark application
    */
  def this (conf: SparkConf) {
    this(new SparkContext(conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")
      .set("spark.kryoserializer.buffer.max", "256MB")))
  }

  def this (uri: String, name: String) {
    this(new SparkConf( ).setMaster(uri).setAppName(name))
  }

  def this (uri: String, name: String, parser: (String) => (String)) {
    this(new SparkConf( ).setMaster(uri).setAppName(parser(name)))
  }

  def setLocalProperty (key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  def stop: Unit = sparkContext.stop( )

  /**
    * Adds http credentials which is then registered by any function
    * using ucar's httpservices. Namely, if you are reading any opendap file
    * that requires you to enter authentication credentials in the browser.
    * Some datasets (like those hosted by gesdisc) require http credentials
    * for authentiation and access.
    *
    * @param uri      webpage url
    * @param username the username used for authentication
    * @param password the password used for authentication
    */
  def addHTTPCredential (uri: String, username: String, password: String): Unit = {
    HTTPCredentials = HTTPCredentials.+:((uri, username, password))
  }

  /**
    * Given an URI string the relevant dataset is loaded into an RDD.
    * If the URI string ends in a .txt and consists of line separated URI's pointing
    * to netCDF files then NetcdfDataset is called. Otherwise NetcdfRandomAccessDatasets
    * is called.
    *
    * @param path0       The URI string pointing to
    *                   A) a directory of netCDF files
    *                   B) a .txt file consisting of line separated OpenDAP URLs
    * @param vars       The variables to be extracted from the dataset
    * @param partitions The number of partitions the data should be split into
    */
  def sciDatasets(path0: String, vars: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDatasetCW] = {
    val path: String = {
      if (path0.endsWith("/")) {
        path0
      } else {
        path0 + "/"
      }
    }
    val uri = new URI(path)

    netcdfRandomAccessDatasets(path, vars, partitions)

  }

  /*def sciDatasetsCW (path : String, vars : List[String] = Nil, partitions : Int = defaultPartitions):RDD[SciDatasetCW] = {
    val uri = new URI(path)

    netcdfRandomAccessDatasets(path, vars, partitions)

  }*/

  /**
    * Constructs an SRDD from a file of URI's pointing to NetCDF datasets and a list of variable names.
    * If no names are provided then all variable arrays in the file are loaded.
    * The URI could be an OpenDapURL or a filesystem path.
    *
    * For reading from HDFS check NetcdfDFSFile.
    */
  def netcdfRandomAccessDatasets (path: String, varName: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDatasetCW] = {

    val fs = FileSystem.get(new URI(path), new Configuration( ))
    val FileStatuses = fs.listStatus(new Path(path))
    val fileNames = FileStatuses.map(p => p.getPath.getName)
    val nameRDD = sparkContext.parallelize(fileNames, partitions)

    nameRDD.map(fileName => {
      val k = NetCDFUtils.loadDFSNetCDFDataSet(path, path + fileName, 4000)
      varName match {
        case Nil => new SciDatasetCW(k)
        case s: List[String] => new SciDatasetCW(k, s)
      }
    })
  }

  /**
    * Constructs an RDD of SciDatasets from a file of URI's pointing to a NetCDF dataset and a list of
    * variable names. If no names are provided then all variable arrays in the file are loaded.
    * The URI could be an OpenDapURL or a filesystem path.
    *
    * For reading from HDFS check NetcdfRandomAccessDatasets
    */
  def netcdfDatasetList (path: String, vars: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDatasetCW] = {

    val creds = HTTPCredentials.clone( )
    val URIsFile = sparkContext.textFile(path, partitions)
    val rdd = URIsFile.map(p => {
      for ((uri, username, password) <- creds) NetCDFUtils.setHTTPAuthentication(uri, username, password)
      val netcdfDataset = NetCDFUtils.loadNetCDFDataSet(p)
      if (vars.nonEmpty) {
        new SciDatasetCW(netcdfDataset, vars)
      } else {
        new SciDatasetCW(netcdfDataset)
      }
    })
    rdd
  }

  /**
    * Constructs an SRDD from a file of URI's pointing to NetCDF datasets and a list of variable names.
    * If no names are provided then all variable arrays in the file are loaded.
    * The URI could be an OpenDapURL or a filesystem path.
    *
    * For reading from HDFS check NetcdfDFSFile.
    */

  /**
    * Constructs an RDD given a URI pointing to an HDFS directory of Netcdf files and a list of variable names.
    * Note that since the files are read from HDFS, the binaryFiles function is used which is called
    * from SparkContext. This is why a normal RDD is returned instead of an SRDD.
    *
    * TODO :: Create an SRDD instead of a normal RDD
    */


  /*def netcdfWholeDatasets(path: String, varNames: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDataset] = {

    val textFiles = sparkContext.binaryFiles(path, partitions)
    textFiles.map(p => {
      val byteArray = p._2.toArray()
      val dataset = loadNetCDFFile(p._1, byteArray)
      varNames match {
        case Nil => new SciDataset(dataset)
        case s => new SciDataset(dataset, varNames)
      }
    })
  }*/

  /**
    * Constructs an RDD given a URI pointing to an HDFS directory of Netcdf files and a list of variable names.
    * Note that since the files are read from HDFS, the binaryFiles function is used which is called
    * from SparkContext.
    * netcdfWholeDatasets should only be used for reading large subsets of variables or all the
    * variables in NetCDF Files.
    *
    * @param path       the path to read from. The format for HDFS directory is : hdfs://HOSTNAME:<port no.>
    * @param varNames   the names of variables to extract. If none are provided, then all variables are reaad.
    * @param partitions The number of partitions to split the dataset into
    * @return
    */

  /*def netcdfWholeDatasets(path: String,idNameMap:mutable.HashMap[String,Array[String]],varNames: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDataset] ={
    val textFiles = sparkContext.binaryFiles(path, partitions)
    textFiles.map(p => {

      val byteArray = p._2.toArray()
      val dataset = loadNetCDFFile(p._1, byteArray)
      val scidata:SciDataset=varNames match {
        case Nil => new SciDataset(dataset)
        case s => new SciDataset(dataset, varNames)
      }

      //testShowOut
      val idName=AttributesOperation.getIDName(scidata)
      val timeStep=AttributesOperation.gettimeStep(scidata)
      val fcType=AttributesOperation.getFcType(scidata)
      val groupStr=MatchSciDatas.idNameTimeStep2KeyShowStr(idNameMap,idName,timeStep)
      println(this.getClass.getCanonicalName+";fcType="+fcType+","+groupStr+";path="+path)

      scidata
    })
  }*/
  def netcdfWholeDatasets (path: String, varNames: List[String] = Nil, partitions: Int = defaultPartitions): RDD[SciDatasetCW] = {
    val textFiles = sparkContext.binaryFiles(path, partitions)
    textFiles.map(p => {
      val byteArray = p._2.toArray( )
      val dataset = loadNetCDFFile(p._1, byteArray)
      varNames match {
        case Nil => new SciDatasetCW(dataset)
        case s => new SciDatasetCW(dataset, varNames)
      }
    })
  }






  /**
    * Reads data from WWLLN files into a SQL Dataframe and broadcasts this DF
    *
    * @param WWLLNpath  Path on HDFS to the data
    * @param partitions The number of paritions to use
    *                   return DataFrame with all the WWLLN data at the location provided
    */
  def readWWLLNData (WWLLNpath: String, partitions: Integer): Broadcast[DataFrame] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val WWLLNdataStrs = sparkContext.textFile(WWLLNpath, partitions).filter(lines => lines.split(",").length != 1)
    val wwllnRDD = WWLLNdataStrs.map(line => line.split(",")).map(p =>
      WWLLN(WWLLNUtils.getWWLLNtimeStr(p(0).trim, p(1).trim), p(2).trim.toDouble, p(3).trim.toDouble,
        p(4).trim.toDouble, p(5).trim.toInt))
    val wwllnDF = wwllnRDD.toDF( )
    val broadcastedWWLLN = sparkContext.broadcast(wwllnDF)
    object GetWWLLNDF {
      def getWWLLNDF = broadcastedWWLLN.value
    }
    broadcastedWWLLN
  }

}