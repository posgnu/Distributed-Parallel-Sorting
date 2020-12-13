package Slave

import java.util.concurrent.atomic.AtomicInteger

import client.RpcClient
import io.grpc.{Server, ServerBuilder}
import msg.msg.{Empty, GreeterGrpc, Metainfo, Pingreq}
import org.apache.logging.log4j.scala.Logging

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
  private var fileTransferFinishCount = new AtomicInteger(0)
  private var metainfoMessageSent = false
  private var finishSortMessageSent = false
  private var sortedComplete = false
  var slaveId: Int = -1
  var inputDirList: List[String] = List[String]()
  var outputDir = ""

  private val client = RpcClient("localhost", 6602)
  private val port = 6603

  def main(args: Array[String]): Unit = {
    type OptionMap = Map[Symbol, Any]

    @scala.annotation.tailrec
    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "-O" :: value :: tail =>
          nextOption(map ++ Map('out -> value.toString), tail)
        case "-I" :: value :: tail =>
          nextOption(map ++ Map('in -> value.toString), tail)
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
    if (options.nonEmpty) {
      inputDirList = options('in).asInstanceOf[List[String]]
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
      println(FileManager.readSamples())
      val reply = Empty()
      Future.successful(reply)
    }

    override def metainfoRpc(req: Metainfo) = {
      throw new NotImplementedError()
      val reply = Empty()
      Future.successful(reply)
    }
  }
}

