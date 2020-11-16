package slave
import java.net._
import java.io._
import scala.io._


object MyClient extends App
{
  val socket = new Socket(InetAddress.getByName("localhost"), 6602)
  var in = new BufferedSource(socket.getInputStream).getLines
  val out = new PrintStream(socket.getOutputStream)
  println("Client initialized:")

  println("Client received: " + in.next)

  socket.close
}
