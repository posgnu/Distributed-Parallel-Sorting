package Slave

import java.io.{File, PrintWriter}

import scala.io.Source

object FileManager {
  def readAll() = {
    val fileList = getListOfFiles("./testData/slave1")

    for (filePath <- fileList) {
      val source = Source.fromFile(filePath)
      for (line <- source.getLines()) {
        val key = line.split(" ")(0)
        println(key)
      }
      source.close()
    }
  }

  def writeInputFileToOutput(sorted: List[String]) = {
    val fileList = getListOfFiles("./testData/slave1")
    val 
    for (filePath <- fileList) {
      val source = Source.fromFile(filePath)
      for (line <- source.getLines()) {
        val key = line.split(" ")(0)

      }
      source.close()
    }
    // val fileList = getListOfFiles("./testData/slave1/output")
    // val fileName = (fileList.map(_.getName.toInt).max + 1).toString
    val writer = new PrintWriter(new File("./testData/slave1/output"+ fileName))

    for (line <- sorted) {
      writer.write(line)
    }
    writer.close()
  }

  def readSamples() = {
    val source = Source.fromFile(getListOfFiles("./testData/slave1")(0))
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