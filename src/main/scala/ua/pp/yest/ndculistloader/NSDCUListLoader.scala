/*
 * Copyright (c) 2018. Yevhen Stuzhnyi
 */

package ua.pp.yest.ndculistloader

import java.util.concurrent.TimeUnit
import java.nio.file.{Paths, Files}

import akka.actor._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import akka.actor.{Actor, ActorSystem, Props, SupervisorStrategy}
import ua.pp.yest.ndculistloader.Reaper.WatchMe


/**
  * Main function object
  * Create ActorSystem and main Actor personETL
  */

object NSDCUListLoader extends App {
  val system: ActorSystem = ActorSystem("sanctionPersonLoader")
  var inFilePath: String = ""
  if (args.length > 0) {
    inFilePath = args(0)
  }
  if (! Files.exists(Paths.get(inFilePath))) {
    throw new Exception("File not found")
  }

  val sysETL = ActorSystem("sysETL")
  val reaper = sysETL.actorOf(Props[ETLActorsReaper])
  val etlActor = sysETL.actorOf(Props(new PersonETL(reaper, inFilePath)), "etlActor")
  reaper ! WatchMe(etlActor)
  Await.ready(sysETL.whenTerminated, Duration(1, TimeUnit.MINUTES))
}

/**
  * Actor for DABOWoTeD shutdown pattern
  */
class ETLActorsReaper extends Reaper {
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
  def allSoulsReaped(): Unit = {
    log.warning("allSoulsReaped")
    context.system.terminate()
  }
}


















