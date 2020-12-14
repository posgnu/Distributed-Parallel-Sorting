package Slave

import org.junit._
/**
 * This class is a test suite for the methods in object FunSets.
 *
 * To run this test suite, start "sbt" then run the "test" command.
 */
class HelloSuite {

  @Test def `file`: Unit = {
    println(FileManager.readSamples())
  }



  @Rule def individualTestTimeout = new org.junit.rules.Timeout(10 * 1000)
}
