package master

import java.net.InetAddress
import java.util.concurrent.atomic.AtomicInteger

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import msg.msg.{Empty, GreeterGrpc, Pingreq, Pingres, MetainfoReq, Samplesreq}

import scala.concurrent.{ExecutionContext, Future}

abstract class State
case class Init() extends State
case class Sample() extends State
case class SortCheck() extends State
case class ShuffleCheck() extends State
case class Success() extends State

object RpcServer {
  var state: State = Init()
  var numberOfSlave = 0
  private var connectionCount = new AtomicInteger(0)
  private var metainfoCount = new AtomicInteger(0)
  private var sortedCount = new AtomicInteger(0)
  private var successCount = new AtomicInteger(0)
  private val port = 6602
  var slaveList: List[String] = List[String]()
  var samplesList: List[String] = List[String]()
  private var slaveRpcClientList: List[RpcClient] = List[RpcClient]()

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("dude, i need at least one parameter")

      throw new IllegalStateException()
    }
    numberOfSlave = args(0).toInt
    println(InetAddress.getLocalHost.getHostAddress + ":" + 6602)

    val server = new RpcServer(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }
}

class RpcServer(executionContext: ExecutionContext) extends Logging { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(RpcServer.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    logger.info("Server started, listening on " + RpcServer.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def pingRpc(req: Pingreq) = {
      // For test - Cannot make grpc connection by local ip address. I do not know why
      var reqIp = ""
      if (req.ip == "192.168.56.1") {
        reqIp = "141.223.175.215"
      } else {
        reqIp = req.ip
      }

      RpcServer.slaveList = RpcServer.slaveList :+ reqIp
      RpcServer.slaveRpcClientList = RpcServer.slaveRpcClientList :+ RpcClient(reqIp, 6603)
      val count = RpcServer.connectionCount.addAndGet(1)
      val slaveId = count - 1

      logger.info(count + " slaves are connected - " + req.ip)
      if (count == RpcServer.numberOfSlave) {
        assert(RpcServer.state == Init())
        RpcServer.state = Sample()
        logger.info("Sample stage")

        // var samplesList = List[String]()
        for (dest <- RpcServer.slaveRpcClientList) {
          dest.sendStartSample()
        }
      }
      Future.successful(Pingres(slaveId))
    }

    override def sendSamples(req: Samplesreq) = {
      val count = RpcServer.metainfoCount.addAndGet(1)
      RpcServer.samplesList = RpcServer.samplesList ::: req.samples.toList

      if (count == RpcServer.numberOfSlave) {
        RpcServer.state = SortCheck()
        logger.info("SortCheck stage")
        var pivots = List[String]()
        val sortedSamples = RpcServer.samplesList.sorted
        for (i <- 1 until RpcServer.numberOfSlave) {
          pivots = pivots :+ sortedSamples(i*10-1)
        }
        for (dest <- RpcServer.slaveRpcClientList) {
          dest.sendMetainfo(pivots)
        }
      }
      Future.successful(Empty())
    }

    override def startSampleRpc(req: Empty) = {
      throw new NotImplementedError()
    }

    override def metainfoRpc(req: MetainfoReq) = {
      throw new NotImplementedError()
    }

    override def finishSortRpc(req: Empty) = {
      val count = RpcServer.sortedCount.addAndGet(1)

      if (count == RpcServer.numberOfSlave) {
        assert(RpcServer.state == SortCheck())
        logger.info("Success individual sorting")
        RpcServer.state = ShuffleCheck()
        logger.info("ShuffleCheck stage")

        for (dest <- RpcServer.slaveRpcClientList) {
          dest.sendStartShuffle()
        }
      }
      Future.successful(Empty())
    }

    override def startShuffle(req: Empty) = {
      throw new NotImplementedError()
    }
  }
}

