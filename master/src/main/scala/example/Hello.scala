package example

import org.apache.logging.log4j.scala.Logging

object Hello extends Greeting with App with Logging {
  println(greeting)
  logger.info("Doing stuff")
}

trait Greeting {
  lazy val greeting: String = "hello"
}
