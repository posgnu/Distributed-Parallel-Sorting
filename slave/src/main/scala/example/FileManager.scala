package Slave

import scala.io.Source

object FileManager {
  def readAll() = {
    val fileList = new java.io.File("./testData/slave1").listFiles

    for (filePath <- fileList) {
      val source = Source.fromFile("./testData/slave1" + filePath)
      for (line <- source.getLines()) {
        val key = line.split(" ")(0)
        println(key)
      }
      source.close()
    }
  }

  def readSamples() = {
    val fileList = new java.io.File("/testData/slave1").list()
    println("filelist" + fileList)
    val source = Source.fromFile("./testData/slave1" + fileList(0))
    println("source" + source)
    val lines = source.getLines().take(10)
    println("line" + lines)

    source.close()
    lines.map(x => x.split(" ")(0)).toList
  }
}