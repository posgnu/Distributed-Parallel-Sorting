package Slave

import java.io.{File, PrintWriter}

import client.RpcClient

import scala.io.Source
import scala.reflect.io.Path
import scala.util.Random

object FileManager {
  def writeToFile(data: Seq[String]) = {
    val filename = RpcServer.outputDir + "/result/" + randomFileName(Path("")).get.toString()
    val outputFileWriter = new PrintWriter(new File(filename))
    for (line <- data) {
      outputFileWriter.println(line)
    }
    outputFileWriter.close()
  }

  def sortAndWrite(data: Seq[String]) = writeToFile(data.sortWith((f, s) => f.split(" ")(0) > s.split(" ")(0)))

  val mergeSort : Iterable[Iterator[String]] => Iterator[String] =
    (fileIterators) => {

      val nonEmptyFiles = fileIterators filter (_.hasNext)
      println(nonEmptyFiles.size)
      nonEmptyFiles
        .map(_.next)
        .toList
        .sortWith((f, s) => f.split(" ")(0) > s.split(" ")(0))
        .toIterator ++ mergeSort(nonEmptyFiles)
    }

  def DomergeSort() = {
    var filenames = getListOfFiles(RpcServer.outputDir + "/received/").map(_.getName)
    println("filename1 " + filenames.toString())
    // sort individual file
    for (name <- filenames) {
      val source = Source.fromFile(RpcServer.outputDir + "/received/" + name)
      source.getLines().grouped(100).foreach(sortAndWrite)
      source.close()
      new File(name).delete()
    }

    filenames = getListOfFiles(RpcServer.outputDir + "/result/").map(_.getName)
    println("filename2 " + filenames.toString())
    var iterList = List[Iterator[String]]()
    var sourceList = List[Source]()
    for (name <- filenames) {
      val source = Source.fromFile(RpcServer.outputDir + "/result/" + name)
      sourceList = sourceList :+ source
      iterList = iterList :+ source.getLines()
    }

    val filename = RpcServer.outputDir + "/" + "output"
    val outputFileWriter = new PrintWriter(new File(filename))

    mergeSort(iterList) foreach (i => outputFileWriter.println(i))
    for (s <- sourceList) {
      s.close()
    }
    outputFileWriter.close()
    println("1111")
  }


  def writeReceivedFile(from: String, lines: List[String]) = {
    val outputFileWriter = new PrintWriter(new File(RpcServer.inputDirList(0) + "/output/received/received_from_slaveId" + from + randomFileName(Path("")).get.toString()))

    for (line <- lines) {
      outputFileWriter.println(line)
    }

    outputFileWriter.close()
  }

  def writeReceivedFileAux(lines: Seq[String]) = {
    val outputFileWriter = new PrintWriter(new File(RpcServer.inputDirList(0) + "/output/received/received_from_slaveId" + RpcServer.slaveId.toString + randomFileName(Path("")).get.toString()))

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
      } else  {
        val source = Source.fromFile(new File(RpcServer.inputDirList(0) + "/output/" + i.toString))
        source.getLines().grouped(100).foreach(writeReceivedFileAux)
        source.close()
        new File(RpcServer.inputDirList(0) + "/output/" + i.toString).delete()
      }
    }
  }


  def writeInputFileToOutput() = {
    val fileList = getListOfFiles(RpcServer.inputDirList(0))
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
        // each file should be sorted
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

  def randomFileName(dir: Path, prefix: String = "", suffix: String = "", maxTries: Int = 10, nameSize: Int = 5): Option[Path] = {
    val alphabet = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ ("_")

    //0.- Expensive calculation!
    def generateName = (1 to nameSize).map(_ => alphabet(Random.nextInt(alphabet.size))).mkString

    //1.- Iterator
    val paths = for(_ <- (1 to maxTries).iterator) yield dir/(prefix + generateName + suffix)

    //2.- Return the first non existent file (path)
    paths.find(!_.exists)
  }
}