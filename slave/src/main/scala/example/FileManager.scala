package Slave

import java.io.{File, PrintWriter}

import client.RpcClient

import scala.io.Source

object FileManager {
  def writeReceivedFile(from: String, lines: List[String]) = {
    val outputFileWriter = new PrintWriter(new File(RpcServer.inputDirList(0) + "/output/received_from_slaveId" + from))

    for (line <- lines) {
      outputFileWriter.println(line)
    }

    outputFileWriter.close()
  }

  def sendOutputToPeers() = {
    for (i <- RpcServer.slaveList.indices) {
      if (i != RpcServer.slaveId) {
        val rpcClientForPeer = RpcClient(RpcServer.slaveList(i), 6603)
        val peerSource = Source.fromFile(new File(RpcServer.inputDirList(0) + "/output/" + i.toString))
        peerSource.getLines().grouped(100).foreach(rpcClientForPeer.sendChunk)

        peerSource.close()
        new File(RpcServer.inputDirList(0) + "/output/" + i.toString).delete()
        rpcClientForPeer.sendFinishSendFile()
      }
    }
  }

  def readAll() = {
    val fileList = getListOfFiles(RpcServer.inputDirList(0) + )

    for (filePath <- fileList) {
      val source = Source.fromFile(filePath)
      for (line <- source.getLines()) {
        val key = line.split(" ")(0)
        println(key)
      }
      source.close()
    }
  }

  def writeInputFileToOutput() = {
    val fileList = getListOfFiles(RpcServer.inputDirList(0) + )
    var outputFileWriter = List[PrintWriter]()
    for (i <- RpcServer.slaveList.indices) {
      outputFileWriter = outputFileWriter :+ new PrintWriter(new File(RpcServer.inputDirList(0) + "/output/" + i.toString))
    }

    for (filePath <- fileList) {
      val source = Source.fromFile(filePath)
      for (line <- source.getLines()) {
        val key = line.split(" ")(0)

        var location = RpcServer.pivots.takeWhile(_ < key).size
        outputFileWriter(location).println(line)
      }
      source.close()
    }
    // val fileList = getListOfFiles("./testData/slave1/output")
    // val fileName = (fileList.map(_.getName.toInt).max + 1).toString

    for (writer <- outputFileWriter) {
      writer.close()
    }
  }

  def readSamples() = {
    val source = Source.fromFile(new File(RpcServer.inputDirList(0) + "/input"))
    val lines = source.getLines().take(10)

    val result = lines.map(x => x.split(" ")(0)).toList
    source.close()
    result
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}