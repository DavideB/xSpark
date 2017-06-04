package org.apache.spark.deploy.worker

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 03/06/2017.
  */
class ControllerPollon(var activeExecutors: Int, val maximumCores: Int) extends Logging {

  type ApplicationId = String
  type Stage = Int
  type Cores = Double

  private var desiredCores = new mutable.HashMap[(ApplicationId, Stage), Cores]()
  private var correctedCores = new mutable.HashMap[(ApplicationId, Stage), Cores]()

  def fix_cores(appId: ApplicationId, stage: Stage, cores: Cores): Cores = {
    desiredCores.synchronized {
      // add your desired number of cores
      desiredCores += ((appId, stage) -> cores)

      // check if all requests have been collected
      if (desiredCores.keySet.size == activeExecutors) {
        computeCorrectedCores()
      } else {
        // wait for others to send core requests
        desiredCores.wait()
      }
    }

    // obtain corrected cores
    correctedCores((appId, stage))
  }

  def increaseActiveExecutors(): Unit = {
    desiredCores.synchronized {
      activeExecutors += 1
      if (desiredCores.keySet.size == activeExecutors) {
        computeCorrectedCores()
      }
    }
  }

  def decreaseActiveExecutors(): Unit = {
    desiredCores.synchronized {
      activeExecutors -= 1
      if (desiredCores.keySet.size == activeExecutors) {
        computeCorrectedCores()
      }
    }
  }

  private def computeCorrectedCores(): Unit = {
    val totalCoresRequested = desiredCores.values.sum
    correctedCores = desiredCores

    // scale requested cores if needed
    if (totalCoresRequested > maximumCores) {
      correctedCores.mapValues(requestedCores => (maximumCores / totalCoresRequested) * requestedCores)
    }
    logInfo("REQUESTED CORES: " + desiredCores.values.toList
          + " TOTAL: " + totalCoresRequested
          + " CORRECTED: " + correctedCores.values.toList)

    desiredCores.notifyAll()
    desiredCores = new mutable.HashMap[(ApplicationId, Stage), Cores]()
  }

}
