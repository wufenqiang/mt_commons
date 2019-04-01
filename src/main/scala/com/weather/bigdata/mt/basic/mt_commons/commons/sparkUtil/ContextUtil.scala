package com.weather.bigdata.mt.basic.mt_commons.commons.sparkUtil

import com.weather.bigdata.mt.basic.mt_commons.commons.PropertiesUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark环境帮助方法
  * Edited by wufenqiang on 2018-3-30
  */
object ContextUtil {
  private val sparkSession:SparkSession={
    val ss:SparkSession={
      if(PropertiesUtil.isPrd){
        setSparkSessionCluster()
      }else{
        setSparkSessionLocal()
      }
    }
    //    ss.conf.set("spark.storage.memoryFraction","0.8")
    //    ss.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//控制各个worker节点之间的混洗数据序列化格式，同时还控制RDD存到磁盘上的序列化格式。需要在使用时注册需要序列化的类型，建议在对网络敏感的应用场景下使用Kryo
    ss
  }
  private val sparkContext:SparkContext={
    val sc=this.sparkSession.sparkContext

    //日志收集等级
    sc.setLogLevel("WARN")
    //    sc.setLogLevel("INFO")
    /* val tmpWrite=PropertiesUtil.getWriteTmp()
     sc.setCheckpointDir(tmpWrite)*/

    sc
  }

  private val sciSparkContextCW: SciSparkContextCW = {
    val ssc = new SciSparkContextCW(sparkContext)
    ssc
  }

  /*    private val sparkConf:SparkConf={
        val sconf:SparkConf=this.sparkContext.getConf
  /*      sconf.set("spark.scheduler.mode","FAIR")
          .set("spark.streaming.concurrentJobs","2")*/
        sconf
      }*/

  private val waitSec:Int=30
  private val streamingContext:StreamingContext={
    //new StreamingContext(this.sparkConf,Seconds(waitSec))
    val ssc=new StreamingContext(this.sparkContext, Seconds(waitSec))
    ssc
  }


  /**
    * 取得SparkSession<br/>
    * <font color="red">job最后需要关闭SparkSession</font>
    * @return
    */
  def getSparkSession(): SparkSession = this.sparkSession


  def getSciSparkContextCW(): SciSparkContextCW = this.sciSparkContextCW

  def getSparkContext():SparkContext=this.sparkContext

  /**
    * 取得StreamingContext
    * @return
    */
  def getStreamingContext(): StreamingContext = this.streamingContext


  /* def main(args: Array[String]): Unit = {
     var a = Array(1,2,3,4,1,2,3,4,1,2,3,4)
     val rdd:RDD[Int] = this.sparkContext.parallelize(a, 3)
     rdd.collect().foreach(println)
   }*/


  /**
    * 取得本地测试SparkSession
    * @return
    */
  private def setSparkSessionLocal(): SparkSession = {
    val sparksession = SparkSession
      .builder()
      .appName("test spi spark lib")
      //.master("local")//数字为启动核心数
      .master("local") //数字为启动核心数
      .config("spark.driver.maxResultSize", "5g")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.kryoserializer.buffer.max", "64m")
      //程序过长需要增加其大小,通常报错状态Cannot allocate new FloatPointer,调节不起作用应该是程序里出现单机类似的死锁情况。

      //.config("spark.driver.LogLevel","WARN")
      .getOrCreate()
    //配置driver的maxResultSize
    //spark.conf.set("spark.driver.maxResultSize","2g")
    sparksession
  }
  /**
    * 集群SparkSession<br/>
    * @return
    */
  private def setSparkSessionCluster(): SparkSession = {
    //    val warehouseLocation = new File("/user/hive/warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("alg-forecast")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "128m")
      //      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      .config("spark.sql.warehouse.dir", warehouseLocation)

      //      .config("spark.submit.deployMode","client")
      //***************************************************************************************************//
      /*//Spark驱动程序的部署模式，即“客户端”或“集群”，这意味着在集群内的某个节点上在本地启动驱动程序（“客户端/client”）或远程启动（“集群/cluster”）
      .config("spark.master","yarn")
      .config("spark.submit.deployMode","cluster")*/


      //耗内存计算公式：“Spark Memory” * spark.memory.storageFraction = (“Java Heap” – “Reserved Memory”) * spark.memory.fraction * spark.memory.storageFraction
      //配置driver
      //spark.driver.memory 无法在此处设置成功，在脚本总设置
      //      .config("spark.driver.memory","5g")//(设置不起作用)spark-driver进程分配的内存大小,也就是执行start-thriftserver.sh机器上分配给thriftserver的内存大小。
      //      .config("spark.driver.memoryOverhead","2g")
      /*.config("spark.driver.maxResultSize","5g")

            .config("spark.yarn.am.memory","7g")
            .config("spark.yarn.am.memoryOverhead","6g")*/


      //      .config("spark.executor.memory","5g") //每个executor进程分配的内存大小
      //      .config("spark.yarn.executor.memoryOverhead","5g")//每个executor在分配的内存之外，能够额外获得的内存的大小，不设置是10%,executorMem= X+max(X*0.1,384)
      //      .config("spark.memory.fraction","0.5")//默认值0.5,这个值越高,执行中可用的内存就越少,并且任务可能更频繁地溢出到磁盘。

      //.config("spark.memory.fraction","0.3")//可保证除了入库以外
      //      .config("spark.memory.fraction","0.1")

      //需设置永久代测试
      //      .config("spark.executor.extraJavaOptions","-XX:MaxPermSize=2g")
      //      .config("spark.driver.extraJavaOptions","-XX:PermSize=2g -XX:MaxPermSize=2g")

      //            .config("spark.shuffle.memoryFraction","0.3")//Shuffle过程中使用的内存达到总内存多少比例的时候开始Spill
      //      .config("spark.shuffle.safetyFraction","0.7")//用来作为一个保险系数，降低实际Shuffle使用的内存阀值

      //      .config("spark.storage.safetyFraction","0.95")
      //      .config("spark.storage.memoryFraction","0.35")//建议值0.6,不改变Store Memory比例,调节RDD缓存占用的内存空间，与算子执行时创建的对象占用的内存空间的比例,如果发现垃圾回收频繁发生,可以将RDD缓存占用空间的比例降低，从而给更多的空间让task创建的对象进行使用。

      //最大并行度=spark.executor.instances*spark.executor.cores
      //Allocated CPU Vcore=spark.executor.instances*spark.executor.cores+1(driver)
      //      .config("spark.executor.instances",55) //num-excutor最多能够同时启动的EXECUTOR的实例个数
      //      .config("spark.executor.cores",1)//(设置不起作用)每个EXECUTOR能够使用的CPU core的数量

      //      .config("yarn.scheduler.minimum-allocation-mb","10240mb")

      //修改reduce任务开始的执行时间
      //      .config("mapreduce.job.reduce.slowstart.completedmaps","0.6")//当map 任务完成的比例达到该值后才会为reduce task申请资源
      //      .config("yarn.app.mapreduce.am.job.reduce.rampup.limit","0.3")//在map任务完成之前，最多启动reduce 任务比例
      //      .config("yarn.app.mapreduce.am.job.reduce.preemption.limit","0.5")//当map task需要资源但暂时无法获取资源（比如reduce task运行过程中，部分map task因结果丢失需重算）时，为了保证至少一个map task可以得到资源，最多可以抢占reduce task比例
      /*.config("mapreduce.job.reduce.slowstart.completed.maps","0.6")*/


      //      .config("mapred.task.timeout", "0")  //不检查超时

      //(2048,2048)/(4096,4096)会报错
      //      .config("mapreduce.map.memory.mb","10240")//每个Map Task需要的内存量
      //      .config("mapreduce.reduce.memory.mb","10240")//每个Reduce Task需要的内存量
      //        .config("yarn.scheduler.maximum-allocation-mb","10240")

      //物理内存与虚拟内存比值,(没有用)
      //.config("yarn.nodemanager.vmem-pmem-ratio",10)
      //      .config("yarn.nodemanager.resource.memory-mb","10240")


      /*//设置hdfs上的依赖jar包地址
      .config(
      "spark.yarn.dist.jars",
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/SciSpark.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/commons-codec-1.9.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/commons-lang3-3.1.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/commons-logging-1.2.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/fastjson-1.2.20.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/httpclient-4.5.3.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/httpcore-4.4.6.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/httpmime-4.5.3.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/javacsv-2.0.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/kvstore-1.6.jar" +","+
        "hdfs://nameservice1/data/dataSource/fc/grid/WEATHER/source/forecastjar/netcdfAll-4.6.11.jar"
    )*/


      /*.config("spark.dynamicAllocation.enabled","true")//是否使用动态资源分配，这种分配可以根据工作负载来扩大和缩小注册到此应用程序的执行程序的数量。
      .config("spark.shuffle.service.enabled","true")//同一集群中的每个工作节点上设置外部洗牌服务*/
      .getOrCreate()

    //spark.conf.set("spark.driver.memory","6g")
    spark
  }

  /*private def setSparkConfCluster()={
    val conf=new SparkConf().setMaster("")
  }*/
  val getApplicationID:String=this.sparkContext.applicationId
  //yarn
  val getMaster:String=this.sparkContext.master
  def getStreamWaitSec:Int=this.waitSec
  def sparkContextMap:Map[String,String]=this.sparkSession.conf.getAll
  def main(args:Array[String]): Unit ={
    val master=this.getMaster
    println(master)
  }
}
