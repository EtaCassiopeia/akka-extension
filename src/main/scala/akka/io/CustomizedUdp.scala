/**
  * Copyright (C) 2009-2016 Typesafe Inc. <http://www.typesafe.com>
  */
package akka.io

import java.io.FileDescriptor
import java.lang.reflect.Field
import java.net.{DatagramSocket, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel
import java.util.Optional

import akka.actor._
import akka.io.Inet.{SoJavaFactories, SocketOption}
import akka.io.Udp.UdpSettings
import sun.nio.ch.Net

import scala.collection.immutable

/**
  * Customized UDP Extension for Akka’s IO layer.this is a modified version of akka.io.Udp class implemented by Typesafe Inc.
  * this implementation adds a new feature called SO_REUSEPORT to the original akka.io.Udp extension
  * and uses plugable RecordFactory to create received messages.
  *
  *
  * This extension implements the connectionless UDP protocol without
  * calling `connect` on the underlying sockets, i.e. without restricting
  * from whom data can be received. For “connected” UDP mode see [[UdpConnected]].
  *
  * For a full description of the design and philosophy behind this IO
  * implementation please refer to <a href="http://doc.akka.io/">the Akka online documentation</a>.
  *
  * The Java API for generating UDP commands is available at [[UdpMessage]].
  *
  * @author Mohsen Zainalpour
  * @version 1.0
  * @since 2/11/16
  */

object CustomizedUdp extends ExtensionId[CustomizedUdp] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): CustomizedUdp = new CustomizedUdp(system)

  override def lookup(): ExtensionId[_ <: Extension] = CustomizedUdp

  override def get(system: ActorSystem): CustomizedUdp = super.get(system)

  sealed trait Message

  trait Command extends SelectionHandler.HasFailureMessage with Message {
    def failureMessage = CommandFailed(this)
  }

  final case class Bind(handler: ActorRef,
                        localAddress: InetSocketAddress, createRecord: (ByteBuffer, InetSocketAddress) => Optional[AnyRef],
                        options: immutable.Traversable[SocketOption] = Nil) extends Command

  case object Unbind extends Command

  case object SuspendReading extends Command

  case object ResumeReading extends Command

  trait Event extends Message

  final case class CommandFailed(cmd: Command) extends Event

  final case class Bound(localAddress: InetSocketAddress) extends Event

  sealed trait Unbound

  case object Unbound extends Unbound

}

class CustomizedUdp(system: ExtendedActorSystem) extends IO.Extension {
  val settings: UdpSettings = new UdpSettings(system.settings.config.getConfig("akka.io.udp"))

  val manager: ActorRef = {
    system.systemActorOf(
      props = Props(classOf[CustomizedUdpManager], this).withDeploy(Deploy.local),
      name = "IO-UDP-CFF")
  }

  def getManager: ActorRef = manager

  private[io] val bufferPool: BufferPool = new DirectByteBufferPool(settings.DirectBufferSize, settings.MaxDirectBufferPoolSize)
}

object CustomizedUdpMessage {

  import java.lang.{Iterable => JIterable}

  import CustomizedUdp._

  import language.implicitConversions
  import scala.collection.JavaConverters._

  def bind(handler: ActorRef, endpoint: InetSocketAddress, createRecord: (ByteBuffer, InetSocketAddress) => Optional[AnyRef], options: JIterable[SocketOption]): Command =
    Bind(handler, endpoint, createRecord, options.asScala.to)

  def unbind: Command = Unbind
}

object CustomizedUdpSO extends SoJavaFactories {

  import java.lang.{Boolean => JBoolean, Integer => JInteger}

  def reusePort() = ReusePort()

  final case class ReusePort() extends Inet.AbstractSocketOption {

    private val SO_REUSEPORT: Int = 15
    private val SOL_SOCKET: Int = 1

    override def beforeDatagramBind(s: DatagramSocket): Unit = setReusePort(s.getChannel)

    def setReusePort(datagramChannel: DatagramChannel): Unit = {
      val fieldFd: Field = datagramChannel.getClass.getDeclaredField("fd")
      fieldFd.setAccessible(true)
      val fd: FileDescriptor = fieldFd.get(datagramChannel).asInstanceOf[FileDescriptor]

      val fileDescriptorClass = classOf[FileDescriptor]
      classOf[Net].getDeclaredMethods.find(_.getName == "setIntOption0").map {
        m =>
          m.setAccessible(true)
          m.getParameterTypes.toSeq match {
            //JDK 1.8.0_66
            case Seq(`fileDescriptorClass`, JBoolean.TYPE, JInteger.TYPE, JInteger.TYPE, JInteger.TYPE, JBoolean.TYPE) => m.invoke(null, fd, false.asInstanceOf[JBoolean], SOL_SOCKET.asInstanceOf[JInteger], SO_REUSEPORT.asInstanceOf[JInteger], 1.asInstanceOf[JInteger], true.asInstanceOf[JBoolean])
            //JDK 1.8.0_31
            case Seq(`fileDescriptorClass`, JBoolean.TYPE, JInteger.TYPE, JInteger.TYPE, JInteger.TYPE) => m.invoke(null, fd, false.asInstanceOf[JBoolean], SOL_SOCKET.asInstanceOf[JInteger], SO_REUSEPORT.asInstanceOf[JInteger], 1.asInstanceOf[JInteger]);
          }
      }
    }
  }
}

