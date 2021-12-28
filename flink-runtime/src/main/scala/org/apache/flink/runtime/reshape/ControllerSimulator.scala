package org.apache.flink.runtime.reshape

import java.util
import java.util.function.Consumer
import java.util.TimerTask
import org.apache.flink.api.common.JobStatus
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.metrics.MetricNames
import org.apache.flink.runtime.reshape.WorkerSimulator.CustomMessage
import org.apache.flink.runtime.rest.handler.legacy.metrics.{MetricFetcher, MetricStore}
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo

import scala.collection.JavaConversions.{iterableAsScalaIterable, mapAsScalaMap}
import scala.collection.mutable

object ControllerSimulator {
  private var metricFetcher: MetricFetcher = null
  private val hiThreshold =
    if(System.getProperty("hiThreshold")!=null){
      System.getProperty("hiThreshold").toDouble
    }else{
     0.8
    }
  private val firstPhaseNumber =
    if(System.getProperty("firstPhaseNum")!=null){
      System.getProperty("firstPhaseNum").toInt
    }else{
      6
    }
  private val loThreshold =
    if(System.getProperty("loThreshold")!=null){
    System.getProperty("loThreshold").toDouble
  }else{
    0.2
  }
  private val reshapeFreq =
    if(System.getProperty("reshapeFreq")!=null){
      System.getProperty("reshapeFreq").toLong
    }else{
      10000L
    }

  def setMetricFetcher(fetcher: MetricFetcher): Unit ={
    metricFetcher = fetcher
  }

  def registerJobToMonitor(executionGraph: ExecutionGraph): Unit ={
    if(System.getProperty("enableReShape")!=null && System.getProperty("enableReShape").equals("false")){
      return
    }
    val t = new java.util.Timer()
    val task: TimerTask = new java.util.TimerTask {
      var appliedMitigation:mutable.HashMap[String, CustomMessage] = mutable.HashMap.empty
      val prevSkewedHelperPairs:mutable.HashMap[String,mutable.HashMap[Int, Int]] = mutable.HashMap.empty
      val ongoingMitigation:mutable.HashMap[String, mutable.HashMap[Int,(Int, Int ,Int, Int)]] = mutable.HashMap.empty
      var mitigationCounter = 0
      def run(): Unit = {
        mitigationCounter+=1
        metricFetcher.update()
        executionGraph.getAllVertices.foreach{
          case (k,v) =>
            println(s"collecting metrics for ${k}, ${v.getName}")
            if(!prevSkewedHelperPairs.contains(k.toString)){
              prevSkewedHelperPairs(k.toString) = mutable.HashMap[Int,Int]()
            }
            if(!ongoingMitigation.contains(k.toString)){
              ongoingMitigation(k.toString) = mutable.HashMap[Int,(Int, Int, Int, Int)]()
            }
            val taskMetricStore = metricFetcher.getMetricStore.getTaskMetricStore(executionGraph.getJobID.toString, k.toString)
            val inputMap = mutable.HashMap[Int, (Int, Double)]()
            val subtaskBackPressureInfo = createSubtaskBackPressureInfo(inputMap, taskMetricStore.getAllSubtaskMetricStores)
            val result = handleOperatorMetrics(v.getName, ongoingMitigation(k.toString), k, inputMap, subtaskBackPressureInfo, mitigationCounter, prevSkewedHelperPairs(k.toString))
            if(result != null){
              if(appliedMitigation.contains(k.toString) && appliedMitigation(k.toString) != result){
                v.getInputs.foreach{
                  x =>
                    println(s"mitigation: ${result} to ${x.getProducer.getJobVertexId.toString}")
                    x.getProducer.sendCustomMessage(result)
                }
              }
              appliedMitigation(k.toString) = result
          }
        }
      }
    }
    t.schedule(task, reshapeFreq, reshapeFreq)
    executionGraph.getTerminationFuture.thenAccept(new Consumer[JobStatus] {
      override def accept(t: JobStatus): Unit = {
        task.cancel()
      }
    })
  }

  def handleOperatorMetrics(name:String, ongoingMitigation:mutable.HashMap[Int,(Int,Int,Int, Int)], operator:JobVertexID, inputMap:mutable.HashMap[Int, (Int,Double)], metrics: util.List[JobVertexBackPressureInfo.SubtaskBackPressureInfo], mitigationCounter:Int, prevSkewHelpers:mutable.HashMap[Int,Int]): CustomMessage ={
    var detectedBusy = false
    val busyWorker:mutable.ArrayBuffer[(Double,Int)] = mutable.ArrayBuffer.empty
    val idleWorker:mutable.ArrayBuffer[(Double,Int)] = mutable.ArrayBuffer.empty
    val workerBusyRatio = new mutable.HashMap[Int, Double]()
    metrics.toSeq.foreach {
      x =>
      println(s"${name}-${operator.toString.substring(0,4)}-${x.getSubtask} BusyRatio ${x.getBusyRatio} InputCount ${inputMap(x.getSubtask)._1} InputRate ${inputMap(x.getSubtask)._2}")
      workerBusyRatio(x.getSubtask) = x.getBusyRatio
      val allocatedHelpers = ongoingMitigation.values.map(_._1).toSet
      if(x.getBusyRatio > hiThreshold && !ongoingMitigation.contains(x.getSubtask)){
        busyWorker.append((x.getBusyRatio, x.getSubtask))
      }else if(x.getBusyRatio < loThreshold && !allocatedHelpers.contains(x.getSubtask)){
        idleWorker.append((x.getBusyRatio, x.getSubtask))
      }
    }
    val toRemove = mutable.HashSet[Int]()
    ongoingMitigation.foreach{
      case (k, v) =>
        if(inputMap(k)._1<inputMap(v._1)._1) {
          toRemove.add(k)
        }
    }
    toRemove.foreach(ongoingMitigation.remove)
    detectedBusy = busyWorker.nonEmpty || ongoingMitigation.nonEmpty
    if(detectedBusy){
      val skewHelperPairs = busyWorker.sortBy(-_._1).map(_._2).zip(idleWorker.sortBy(_._1).map(_._2)).map{
        case (skewed,helper) => if(prevSkewHelpers.contains(skewed)){
          (skewed, prevSkewHelpers(skewed))
        }else{
          prevSkewHelpers(skewed) = helper
          (skewed, helper)
        }
      }.toArray
      skewHelperPairs.foreach{
        case (skewed, helper) =>
          ongoingMitigation(skewed) = (helper, 90,100, 0)
      }
      ongoingMitigation.keySet.foreach {
        k =>
          val iteration = ongoingMitigation(k)._4
          val helper = ongoingMitigation(k)._1
          val ratio = if (iteration < firstPhaseNumber) {
            90
          } else {
            ((inputMap(k)._1 - inputMap(helper)._1) * 100 / inputMap(k)._1)
          }
          ongoingMitigation(k) = (helper, ratio, 100, iteration + 1)
      }

      val finalPairs = mutable.ArrayBuffer[(Int,Int)]()
      val finalRatios = mutable.ArrayBuffer[(Int,Int)]()
      ongoingMitigation.foreach{
        case (k,v) =>
          finalPairs.append((k,v._1))
          finalRatios.append((v._2,v._3))
      }
      return CustomMessage(detectedBusy, finalPairs.toArray, finalRatios.toArray)
    }
    CustomMessage(detectedBusy, Array.empty, Array.empty)
  }

  private def createSubtaskBackPressureInfo(inputMaps:mutable.HashMap[Int,(Int,Double)], subtaskMetricStores: util.Map[Integer, MetricStore.ComponentMetricStore]): util.List[JobVertexBackPressureInfo.SubtaskBackPressureInfo] = {
    val result: util.List[JobVertexBackPressureInfo.SubtaskBackPressureInfo] = new util.ArrayList[JobVertexBackPressureInfo.SubtaskBackPressureInfo](subtaskMetricStores.size)
    import scala.collection.JavaConversions._
    for (entry <- subtaskMetricStores.entrySet) {
      val subtaskIndex: Int = entry.getKey
      val subtaskMetricStore: MetricStore.ComponentMetricStore = entry.getValue
      val inputCount = subtaskMetricStore.getMetric(MetricNames.IO_NUM_RECORDS_IN, "0").toInt
      val inputRate = subtaskMetricStore.getMetric(MetricNames.IO_NUM_RECORDS_IN_RATE, "0").toDouble
      inputMaps(subtaskIndex) = (inputCount, inputRate)
      val backPressureRatio: Double = getBackPressureRatio(subtaskMetricStore)
      val idleRatio: Double = getIdleRatio(subtaskMetricStore)
      val busyRatio: Double = getBusyRatio(subtaskMetricStore)
      result.add(new JobVertexBackPressureInfo.SubtaskBackPressureInfo(subtaskIndex, getBackPressureLevel(backPressureRatio), backPressureRatio, idleRatio, busyRatio))
    }
    result
  }

  private def getBackPressureRatio(metricStore: MetricStore.ComponentMetricStore): Double = getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_BACK_PRESSURED_TIME)

  private def getIdleRatio(metricStore: MetricStore.ComponentMetricStore): Double = getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_IDLE_TIME)

  private def getBusyRatio(metricStore: MetricStore.ComponentMetricStore): Double = getMsPerSecondMetricAsRatio(metricStore, MetricNames.TASK_BUSY_TIME)

  private def getMsPerSecondMetricAsRatio(metricStore: MetricStore.ComponentMetricStore, metricName: String): Double = (metricStore.getMetric(metricName, "0")).toDouble / 1000

  private def getBackPressureLevel(backPressureRatio: Double): JobVertexBackPressureInfo.VertexBackPressureLevel = {
    if (backPressureRatio <= 0.10) JobVertexBackPressureInfo.VertexBackPressureLevel.OK
    else if (backPressureRatio <= 0.5) JobVertexBackPressureInfo.VertexBackPressureLevel.LOW
    else JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH
  }


}
