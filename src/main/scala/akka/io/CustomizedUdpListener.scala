/**
  * Copyright (C) 2009-2016 Typesafe Inc. <http://www.typesafe.com>
  */
package akka.io

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey._

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.dispatch.{UnboundedMessageQueueSemantics, RequiresMessageQueue}
import akka.io.CustomizedUdp._
import akka.io.Inet.DatagramChannelCreator
import akka.io.SelectionHandler.ChannelReadable

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
  * This is a modified version of akka.io.UdpListener class implemented by Typesafe Inc.
  *
  * @author Mohsen Zainalpour
  * @version 1.0
  * @since 2/11/16
  */

class CustomizedUdpListener(val udp: CustomizedUdp,
                            channelRegistry: ChannelRegistry,
                            bindCommander: ActorRef,
                            bind: Bind
                           )
  extends Actor with ActorLogging with RequiresMessageQueue[UnboundedMessageQueueSemantics] {

  import udp.bufferPool
  import udp.settings._

  def selector: ActorRef = context.parent

  context.watch(bind.handler)
  // sign death pact

  val channel = bind.options.collectFirst {
    case creator: DatagramChannelCreator => creator
  }.getOrElse(DatagramChannelCreator()).create()
  channel.configureBlocking(false)

  val createRecord = bind.createRecord

  val localAddress =
    try {
      val socket = channel.socket
      bind.options.foreach(_.beforeDatagramBind(socket))
      socket.bind(bind.localAddress)
      val ret = socket.getLocalSocketAddress match {
        case isa: InetSocketAddress => isa
        case x => throw new IllegalArgumentException(s"bound to unknown SocketAddress [$x]")
      }
      channelRegistry.register(channel, OP_READ)
      log.debug("Successfully bound to [{}]", ret)
      bind.options.foreach {
        case o: Inet.SocketOptionV2 => o.afterBind(channel.socket)
        case _ =>
      }
      ret
    } catch {
      case NonFatal(e) =>
        bindCommander ! CommandFailed(bind)
        log.error(e, "Failed to bind UDP channel to endpoint [{}]", bind.localAddress)
        context.stop(self)
    }

  def receive: Receive = {
    case registration: ChannelRegistration =>
      bindCommander ! Bound(channel.socket.getLocalSocketAddress.asInstanceOf[InetSocketAddress])
      context.become(readHandlers(registration), discardOld = true)
  }

  def readHandlers(registration: ChannelRegistration): Receive = {
    case SuspendReading => registration.disableInterest(OP_READ)
    case ResumeReading => registration.enableInterest(OP_READ)
    case ChannelReadable => doReceive(registration, bind.handler)

    case Unbind =>
      log.debug("Unbinding endpoint [{}]", bind.localAddress)
      try {
        channel.close()
        sender() ! Unbound
        log.debug("Unbound endpoint [{}], stopping listener", bind.localAddress)
      } finally context.stop(self)
  }

  def doReceive(registration: ChannelRegistration, handler: ActorRef): Unit = {
    @tailrec def innerReceive(readsLeft: Int, buffer: ByteBuffer) {
      buffer.clear()
      buffer.limit(DirectBufferSize)

      channel.receive(buffer) match {
        case sender: InetSocketAddress =>
          buffer.flip()
          handler ! createRecord(buffer, sender)
          if (readsLeft > 0) innerReceive(readsLeft - 1, buffer)
        case null => // null means no data was available
      }
    }

    val buffer = bufferPool.acquire()
    try innerReceive(BatchReceiveLimit, buffer) finally {
      bufferPool.release(buffer)
      registration.enableInterest(OP_READ)
    }
  }

  override def postStop(): Unit = {
    if (channel.isOpen) {
      log.debug("Closing DatagramChannel after being stopped")
      try {
        channel.close()
      } catch {
        case NonFatal(e) => log.debug("Error closing DatagramChannel: {}", e)
      }
    }
  }
}
