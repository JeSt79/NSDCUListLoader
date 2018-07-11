/*
 * Copyright (c) 2018. Yevhen Stuzhnyi
 */

package ua.pp.yest.ndculistloader

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import ua.pp.yest.ndculistloader.Reaper.WatchMe


//TODO make refactoring for use Akka Typed
case object Start
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
  * Start Actor
  * Exctract data from CSV-file
  * Create actors for data transform and actor for data load
  * Run transform and load data task flow
  */
class PersonETL(reaper: ActorRef, val sourceFileName: String, val transformersCount: Int = 3) extends Actor with ActorLogging {

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
      //log.warning("PersonETL:Start,{}", sourceFileName)
      //Create Loader actor
      val personLoader = context.actorOf(PersonDMLLoader.props(sourceFileName.substring(0, sourceFileName.lastIndexOf('.')) + ".pdc"), "personLoader")

      //Create pool of transformer actors

//      val personTransformers: Array[ActorRef] = new Array[ActorRef](transformersCount)
//      for (i <- 0 until transformersCount) {
//        personTransformers(i) = context.actorOf(PersonTransformer.props(personLoader,transformersCount), s"persontransformer${i + 1}")
//        reaper ! WatchMe(personTransformers(i))
//      }


      val personTransformer = context.actorOf(PersonTransformer.propsWithBalancingPoolRouter(personLoader,transformersCount))
       //Read data from csv-file
      val listReader = CSVReader.open(sourceFileName, "windows-1251")
      //var rowCounter = 0
      try {
        for (personRow <- listReader) {
          val person: SanctionPerson = personRow match {
            case List(orderNo, personType, personNo, personData, sanctionData, sanctionTerm, _*) =>
              new SanctionPerson(orderNo, personType, personNo, personData, sanctionData, sanctionTerm)
          }
          //Send Data to Transformer
          //personTransformers(rowCounter % transformersCount) ! person
          personTransformer ! person
          //rowCounter += 1
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

object PersonETL{
  def props(reaper: ActorRef, sourceFileName: String,transformersCount: Int = 3): Props =
    Props(new PersonETL(reaper, sourceFileName,transformersCount))
}