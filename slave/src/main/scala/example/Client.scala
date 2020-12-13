package client

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import Slave.RpcServer
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import msg.msg.GreeterGrpc.GreeterBlockingStub
import msg.msg.{Empty, GreeterGrpc, Pingreq}
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

  /** Say hello to server. */
  def sendPing(): Unit = {
    logger.info("Will try to connect")
    try {
      val response = blockingStub.pingRpc(Pingreq(InetAddress.getLocalHost.getHostAddress))
      RpcServer.slaveId = response.id
      logger.info("Ping success with id: " + RpcServer.slaveId)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: ping")
    }
  }
}