package master

import java.io._
import java.net._

object TcpServer extends App
{
  try
    {
      val server = new ServerSocket(6602)
      println("TCP server initialized: " + server.getInetAddress.getHostAddress + ":" + server.getLocalPort)
      val client = server.accept
      println("Slave: " + client.getInetAddress.getHostAddress + ":" + client.getLocalPort)

      val in = new BufferedReader(new InputStreamReader(client.getInputStream)).readLine
      val out = new PrintStream(client.getOutputStream)

      println("Server received:" + in)
      out.println("Message received")
      out.flush

      if (in.equals("Disconnect")) client.close; server.close; println("Server closing:")
    }

  catch
    {
      case e: Exception => println(e.getStackTrace); System.exit(1)
    }
}
