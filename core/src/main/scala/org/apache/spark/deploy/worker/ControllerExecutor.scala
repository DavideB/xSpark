
package org.apache.spark.deploy.worker


import java.util.TimerTask
import java.util.Timer
import org.apache.spark.Logging

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerExecutor
   (executorId: String, deadline: Long,
    coreMin: Int, coreMax: Int, _tasks: Int, core: Int) extends Logging {

  val K: Int = 50
  val Ts: Int = 2000 // Sampling Time
  val Ti: Int = 10000
  val CQ: Double = 0.5 // Cores Quantum
  val tasks: Int = _tasks
  var worker: Worker = _

  var csiOld: Double = core.toDouble
  var SP: Double = 0.0
  var completedTasks: Double = 0.001
  var cs: Double = 0.0

  val timer = new Timer()
  var oldCore = core

  def function2TimerTask(f: () => Unit): TimerTask = new TimerTask {
    def run() = f()
  }

  def start(): Unit = {
    def timerTask() = {
      if (SP < 1) SP += Ts / deadline.toDouble
      val nextCore: Int = nextAllocation()
      if (nextCore != oldCore) {
        oldCore = nextCore
        worker.onScaleExecutor("", executorId, nextCore)
      }

      logInfo("SP Updated: " + (SP - (SP % 0.01)).toString)
      logInfo("Real: " + (completedTasks/tasks).toInt.toString)
      logInfo("CoreToAllocate: " + nextCore.toString)
      logInfo("Completed Task: " + completedTasks.toInt.toString)
    }
    timer.schedule(function2TimerTask(timerTask), 0, Ts)
  }

  def stop(): Unit = {
    timer.cancel()
  }

  def nextAllocation(statx: Int = 3): Int = {
    val csp = K * (SP - (completedTasks / tasks))
    if (statx != 3) {
      cs = coreMin
    }
    else {
      val csi = csiOld + K * (Ts / Ti) * (SP - (completedTasks / tasks))
      cs = math.min(coreMax, math.max(coreMin, math.ceil(csp + csi)))
    }
    cs = math.ceil(cs / CQ) * CQ
    csiOld = cs - csp
    math.ceil(cs).toInt
  }

}
