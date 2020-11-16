package master

import java.io._
import java.net._

object TcpServer extends App
{
  try
    {
      val server = new ServerSocket(6602)
      println("TCP server initialized: " + server.getInetAddress.getHostAddress + ":" + server.getLocalPort)

      val numberOfSlave = 1
      for (_ <- 1 to numberOfSlave) {
        val client = server.accept
        println("Slave: " + client.getInetAddress.getHostAddress + ":" + client.getLocalPort)

        new Thread() {
          override def run(): Unit = {
            val out = new PrintStream(client.getOutputStream, true)
            // val in = new BufferedReader(new InputStreamReader(client.getInputStream)).readLine

            val content = "success"

            out.println(content)
            out.flush

            client.close()
          }
        }.start()
      }
      server.close
    }

  catch
    {
      case e: Exception => println(e.getStackTrace); System.exit(1)
    }
}
