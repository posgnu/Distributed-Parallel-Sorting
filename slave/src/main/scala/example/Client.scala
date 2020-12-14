package client

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import Slave.{FileManager, RpcServer}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import msg.msg.GreeterGrpc.GreeterBlockingStub
import msg.msg.GreeterGrpc.GreeterStub
import msg.msg.{Empty, FileChunk, GreeterGrpc, Pingreq, Samplesreq}
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

  def sendSamples() = {
    try {
      val samples = FileManager.readSamples()
      val response = blockingStub.sendSamples(Samplesreq(samples))
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: finishSort")
    }
  }

  def sendFinshSort() = {
    try {
      val response = blockingStub.finishSortRpc(Empty())
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: finishSort")
    }
  }

  def sendChunk(lines: Seq[String]) = {
    try {
      val response = blockingStub.sendFile(FileChunk(lines, RpcServer.slaveId))
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: sendFile")
    }
  }

  def sendFinishSendFile() = {
    try {
      val response = blockingStub.finishSendFile(Empty())
    }
    catch {
      case e: StatusRuntimeException =>
        logger.info("RPC failed: FinishSendFile")
    }
  }
}