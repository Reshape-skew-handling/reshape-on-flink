package org.apache.flink.runtime.reshape

import org.apache.flink.runtime.reshape.WorkerSimulator.CustomMessage

import scala.collection.mutable

object WorkerSimulator{
  case class CustomMessage(setActive:Boolean, skewHelperPairs:Array[(Int,Int)], skewRerouteRatios:Array[(Int,Int)]){
    override def toString: String = {
      s"Active: $setActive (skewed,helper): [${skewHelperPairs.mkString(",")}], (toHelper,toAll): [${skewRerouteRatios.mkString(",")}]"
    }
  }
}

class WorkerSimulator(subTaskId: String) {

  var isActive = false
  var ratios: Map[Int, (Int,Int)] = Map.empty
  var skewedToHelper: Map[Int, Int] = Map.empty
  var currentCounts: mutable.HashMap[Int, Int] = mutable.HashMap.empty

  def changeRouting(targetDest:Int): Int ={
    synchronized{
      if(isActive && skewedToHelper.contains(targetDest)){
        if(!currentCounts.contains(targetDest)){
          currentCounts(targetDest) = 0
        }
        val finalDest = if(currentCounts(targetDest) < ratios(targetDest)._1){
          skewedToHelper(targetDest)
        }else{
          targetDest
        }
        currentCounts(targetDest) += 1
        if(currentCounts(targetDest) >= ratios(targetDest)._2){
          currentCounts(targetDest) = 0
        }
        finalDest
      }else{
        targetDest
      }
    }
  }

  def assignRouting(customMessage: CustomMessage): Unit ={
    println(s"$subTaskId receives $customMessage")
    synchronized{
      isActive = customMessage.setActive
      skewedToHelper = customMessage.skewHelperPairs.toMap
      ratios = customMessage.skewRerouteRatios.indices.map{
        idx =>
          val skewedDest =  customMessage.skewHelperPairs(idx)._1
          currentCounts(skewedDest) = 0
          skewedDest -> customMessage.skewRerouteRatios(idx)
      }.toMap
    }
  }

}
