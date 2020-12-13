package master

import java.util.concurrent.TimeUnit

import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import msg.msg.GreeterGrpc.GreeterBlockingStub
import msg.msg.{Empty, GreeterGrpc}
import org.apache.logging.log4j.scala.Logging

object RpcClient extends Logging {
  def apply(host: String, port: Int): RpcClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build
    val blockingStub = GreeterGrpc.blockingStub(channel)
    new RpcClient(channel, blockingStub)
  }
}

class RpcClient private(
                         private val channel: ManagedChannel,
                         private val blockingStub: GreeterBlockingStub
                       ) extends Logging {

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendStartSample(): Unit = {
    try {
      val response = blockingStub.startSampleRpc(Empty())
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: startsample")
    }
  }

}