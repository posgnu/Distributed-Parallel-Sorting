package master

import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import msg.msg.GreeterGrpc.{GreeterBlockingStub, GreeterStub}
import msg.msg.{Empty, GreeterGrpc, Samplesres, MetainfoReq}
import org.apache.logging.log4j.scala.Logging

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

  def sendStartSample(): Samplesres = {
    try {
      val response : Samplesres =  blockingStub.startSampleRpc(Empty())
      return response
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
      stub.metainfoRpc(MetainfoReq(RpcServer.slaveList, pivots))
    }
    catch {
      case e: StatusRuntimeException => {
        logger.info("RPC failed: startsample")
        throw new IllegalStateException()
      }
    }
  }
}