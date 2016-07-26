
package org.apache.spark.deploy.worker

import org.apache.spark.rpc._
import org.apache.spark._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerProxy
      (rpcEnvWorker: RpcEnv, val driverUrl: String, val execId: Int) {

  var proxyEndpoint: RpcEndpointRef = _
  val ENDPOINT_NAME: String =
    "ControllerProxy-%s".format(driverUrl.split(":").last + "-" + execId.toString)
  var executorRemainingTask: Int = _

  val conf = new SparkConf
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create("Controller", rpcEnvWorker.address.host, 5555, conf, securityMgr)


  def start() {
    proxyEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, createProxyEndpoint(driverUrl))
    // rpcEnv.awaitTermination()
  }

  protected def createProxyEndpoint(driverUrl: String): ProxyEndpoint = {
    new ProxyEndpoint(rpcEnv, driverUrl)
  }

  def getAddress: String = {
    "spark://" + ENDPOINT_NAME + "@" + proxyEndpoint.address.toString
  }


  class ProxyEndpoint(override val rpcEnv: RpcEnv,
                      driverUrl: String) extends ThreadSafeRpcEndpoint with Logging {

    protected val executorIdToAddress = new HashMap[String, RpcAddress]

    private val executorRefMap = new HashMap[String, RpcEndpointRef]

    @volatile var driver: Option[RpcEndpointRef] = Some(rpcEnv.setupEndpointRefByURI(driverUrl))

    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) =>
        if (TaskState.isFinished(state)) {
          executorRemainingTask -= 1
          if (executorRemainingTask <= 0) {
            driver.get.send(ExecutorFinishedTask(executorId))
          }
        }
        driver.get.send(StatusUpdate(executorId, taskId, state, data))

      case RegisteredExecutor(hostname) =>
        executorRefMap(executorIdToAddress(execId.toString).host).send(RegisteredExecutor(hostname))

      case RegisterExecutorFailed(message) =>
        executorRefMap(
          executorIdToAddress(execId.toString).host).send(RegisterExecutorFailed(message))

      case LaunchTask(task) =>
        executorRefMap(executorIdToAddress(execId.toString).host).send(LaunchTask(task))

      case StopExecutor =>
          logInfo("Asked to terminate Executor")
      //  executorRefMap(executorIdToAddress(execId.toString).host).send(StopExecutor)
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        logInfo("Forward connection to driver: " + driverUrl)
        rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          driver = Some(ref)
          logInfo(ref.address.toString)
          ref.ask[RegisterExecutorResponse](
            RegisterExecutor(executorId, self, hostPort, cores, logUrls))
        }(ThreadUtils.sameThread).onComplete {
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          case Success(msg) => Utils.tryLogNonFatalError {
            Option(self).foreach(_.send(msg)) // msg must be RegisterExecutorResponse
          }
          case Failure(e) => {
            logError(s"Cannot register with driver: $driverUrl", e)
            System.exit(1)
          }
        }(ThreadUtils.sameThread)
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }
        executorIdToAddress(executorId) = executorAddress
        this.synchronized {
          executorRefMap.put(executorAddress.host, executorRef)
        }
      case RetrieveSparkProps =>
        val sparkProperties = driver.get.askWithRetry[Seq[(String, String)]](RetrieveSparkProps)
        context.reply(sparkProperties)
    }
  }

}