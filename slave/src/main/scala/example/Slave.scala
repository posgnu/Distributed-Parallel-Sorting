package Slave

import java.util.concurrent.atomic.AtomicInteger

import client.RpcClient
import io.grpc.{Server, ServerBuilder}
import msg.msg.{Empty, FileChunk, GreeterGrpc, MetainfoReq, Pingreq, Samplesreq}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

abstract class State
case class Init() extends State
case class Sample() extends State
case class Sort() extends State
case class Shuffle() extends State
case class Success() extends State
case class Fail() extends State

object RpcServer {
  var state: State = Init()
  var fileTransferFinishCount = new AtomicInteger(0)
  val pool = java.util.concurrent.Executors.newFixedThreadPool(100)
  private var metainfoMessageSent = false
  private var finishSortMessageSent = false
  private var sortedComplete = false
  var slaveList: List[String] = List[String]()
  var pivots: List[String] = List[String]()
  var slaveId: Int = -1
  var inputDirList: List[String] = List[String]()
  var outputDir = ""

  var client: RpcClient = null
  private val port = 6603

  def main(args: Array[String]): Unit = {
    type OptionMap = Map[Symbol, Any]

    @scala.annotation.tailrec
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "-M" :: value :: tail =>
          nextOption(map ++ Map('master -> value.toString), tail)
        case "-O" :: value :: tail =>
          nextOption(map ++ Map('out -> value.toString), tail)
        case "-I" :: value :: tail =>
          nextOption(map ++ Map('in -> List(value.toString)), tail)
        case string :: tail => {
            if (isSwitch(string)) {
              println("Unknown option: " + string)
              throw new IllegalStateException()
            } else {
              nextOption(map ++ Map('in -> (string :: map('in).asInstanceOf[List[String]])), tail)
            }
          }
        }
      }
    val options = nextOption(Map(), args.toList)
    client = RpcClient(options('master).asInstanceOf[String], 6602)
    if (options.nonEmpty) {
      inputDirList = options('in).asInstanceOf[List[String]]
      assert(options('out).asInstanceOf[String] == inputDirList(0)+"/output")
      outputDir = options('out).asInstanceOf[String]
    }


    val server = new RpcServer(ExecutionContext.global)
    server.start()
    client.sendPing()

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
      throw new NotImplementedError()
    }

    override def startSampleRpc(req: Empty) = {
      logger.info("Get startSampleMsg")
      RpcServer.client.sendSamples()

      Future.successful(Empty())
    }

    override def sendSamples(req: Samplesreq) = {
      throw new NotImplementedError()
    }

    override def metainfoRpc(req: MetainfoReq) = {
      logger.info("Get metainfo from master")
      RpcServer.slaveList = req.slaves.toList
      RpcServer.pivots = req.pivots.toList
      logger.info("pivots: " + RpcServer.pivots.toString())
      logger.info("peers: " + RpcServer.slaveList)

      // sorting individually
      FileManager.writeInputFileToOutput()
      logger.info("Finish individual sort")

      // Send Finish message
      RpcServer.client.sendFinshSort()

      Future.successful(Empty())
    }

    override def finishSortRpc(req: Empty) = {
      throw new NotImplementedError()
    }

    override def startShuffle(req: Empty) = {
      logger.info("Get startShuffle message!")

      FileManager.sendOutputToPeers()
      Future.successful(Empty())
    }

    override def sendFile(req: FileChunk) = {
      FileManager.writeReceivedFile(req.id.toString, req.chunk.toList)

      Future.successful(Empty())
    }

    override def finishSendFile(req: Empty) = {
      val count = RpcServer.fileTransferFinishCount.addAndGet(1)

      if (count == RpcServer.slaveList.size - 1) {
        RpcServer.pool.execute(
          new Runnable {
            def run: Unit = {
              logger.info("Finish to receive file from peers")
              FileManager.DomergeSort()
              logger.info("Send success message")
              RpcServer.client.sendSuccess()
            }
          }
        )
      }



      Future.successful(Empty())
    }

    override def success(req: Empty) = {
      throw new NotImplementedError()
    }
  }
}

