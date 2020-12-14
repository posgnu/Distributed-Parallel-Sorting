package master

import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import msg.msg.GreeterGrpc.{GreeterBlockingStub, GreeterStub}
import msg.msg.{Empty, GreeterGrpc, MetainfoReq}
import org.apache.logging.log4j.scala.Logging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Failure

object RpcClient extends Logging {
  def apply(host: String, port: Int): RpcClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.blockingStub(channel)
    val stub = GreeterGrpc.stub(channel)
    new RpcClient(channel, blockingStub, stub)
  }
}

class RpcClient private(
                         private val channel: ManagedChannel,
                         private val blockingStub: GreeterBlockingStub,
                         private val stub: GreeterStub
                       ) extends Logging {

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendStartSample() = {
    try {
      blockingStub.startSampleRpc(Empty())
    }
    catch {
      case e: StatusRuntimeException => {
        logger.info("RPC failed: startsample")
        throw new IllegalStateException()
      }
    }
  }

  def sendMetainfo(pivots: List[String]): Unit = {
    try {
      blockingStub.metainfoRpc(MetainfoReq(RpcServer.slaveList, pivots))
    }
    catch {
      case e: StatusRuntimeException => {
        logger.info("RPC failed: startsample")
        throw new IllegalStateException()
      }
    }
  }

  def sendStartShuffle(): Unit = {
    try {
      blockingStub.startShuffle(Empty())
    }
    catch {
      case e: StatusRuntimeException => {
        logger.info("RPC failed: startsample")
        throw new IllegalStateException()
      }
    }
  }
}