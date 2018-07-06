/*
 * Copyright (c) 2018. Yevhen Stuzhnyi
 */

package ua.pp.yest.ndculistloader

import java.util.concurrent.TimeUnit

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
  val sysETL = ActorSystem("NSDCUListETL")
  val reaper = system.actorOf(Props[ETLActorsReaper])
  val etlActor = sysETL.actorOf(Props(new PersonETL(reaper, inFilePath)), "personETL")
  reaper ! WatchMe(etlActor)
  Await.ready(sysETL.whenTerminated, Duration(1, TimeUnit.MINUTES))
}

/**
  * Actor for DABOWoTeD shutdown pattern
  */
class ETLActorsReaper extends Reaper {
  def allSoulsReaped(): Unit = context.system.terminate()

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
}


//TODO make refactoring for use Akka Typed

case object Start

/**
  * Start Actor
  * Exctract data from CSV-file
  * Create actors for data transform and actor for data load
  * Run transform and load data task flow
  */
class PersonETL(reaper: ActorRef, val sourceFileName: String, val transformersCount: Int = 3) extends Actor with ActorLogging {

  import akka.actor.SupervisorStrategy.Stop
  import com.github.tototoshi.csv._

  implicit object MyFormat extends DefaultCSVFormat {
    override val delimiter = ';'
  }

  override def preStart(): Unit = {
    self ! Start
  }

  override def receive: Receive = {

    case Start => {
      //Start ETL process
      log.debug("PersonETL:Start,{}", sourceFileName)
      //Create Loader actor
      val personLoader = context.actorOf(Props(new PersonDMLLoader(sourceFileName.substring(0, sourceFileName.lastIndexOf('.')) + ".pdc")), "personloader")

      //Create pool of transformer actors
      val personTransformers: Array[ActorRef] = new Array[ActorRef](transformersCount)
      for (i <- 0 until transformersCount) {
        personTransformers(i) = context.actorOf(Props(new PersonTransformer(personLoader)), s"persontransformer${i + 1}")
        reaper ! WatchMe(personTransformers(i))
      }
       //Read data from csv-file
      val listReader = CSVReader.open(sourceFileName, "windows-1251")
      var rowCounter = 0
      try {
        for (personRow <- listReader) {
          val person: SanctionPerson = personRow match {
            case List(orderNo, personType, personNo, personData, sanctionData, sanctionTerm, _*) =>
              new SanctionPerson(orderNo, personType, personNo, personData, sanctionData, sanctionTerm)
          }
          //Send Data to Transformer
          personTransformers(rowCounter % transformersCount) ! person
          rowCounter += 1
        }
      }
      catch {
        case unknown: Throwable => log.error(unknown, "UnknownError")
      }
      finally {
        listReader.close()
      }
    }
  }

}

/**
  * Person data used as Message type
  */
case class SanctionPerson(
                           decreeNo: String,
                           personType: String,
                           listNumber: String,
                           personData: String,
                           sanctionData: String,
                           sancTerm: String,
                           listType: String = "NSDCU_V2",
                           namesString: String = null
                         )

/**
  * Transform Person data before loading process
  * several person data can be transformed concurrency
  */
class PersonTransformer(val personLoader: ActorRef) extends Actor with ActorLogging {
  override def receive = {
    case person: SanctionPerson => {
      try {
        //TODO Implement person names parsing into names string
        personLoader ! person
      }
      catch {
        case anyException: Throwable => log.error(anyException, s"Person number: {${person.listNumber}}")
      }
    }
    case msg => log.warning(s"Unexpected: $msg")
  }
}

/**
  * Actor generates DML-expression as calling of stored procedure in Oracle DB
  * and save it to the script file
  */
class PersonDMLLoader(val scriptFileName: String) extends Actor with ActorLogging {

  override def receive = {
    case person: SanctionPerson => {
      try
        loadPerson(person)
      catch {
        case anyException: Throwable => log.error(anyException, s"Person number: {${person.listNumber}}")
      }
    }
    case msg => log.warning(s"Unexpected: $msg")
  }


  def loadPerson(person: SanctionPerson): Unit = {
    import java.io.{FileOutputStream, OutputStreamWriter}
    val fos = new FileOutputStream(scriptFileName, true)
    val writer = new OutputStreamWriter(fos, "windows-1251")
    try {
      writer.append("fm_rnbo_list_imp.fm_imp_rnbo_list_rec(\n")
      writer.append(s"p_list_type_code => '${person.listType}',\n")
      writer.append(s"p_decree_no => '${person.decreeNo}',\n")
      writer.append(s"p_list_number => '${person.listNumber}',\n")
      writer.append(s"p_entry_type_code => '${person.personType}',\n")
      writer.append(s"p_person_data => '${person.personData.replaceAll("'", "''")}',\n")
      writer.append(s"p_sanc_list => '${person.sanctionData.replaceAll("'", "''")}',\n")
      writer.append(s"p_term => '${person.sancTerm}'\n")
      writer.append(");\n\n")
    }
    finally {
      writer.close()
    }
  }
}









