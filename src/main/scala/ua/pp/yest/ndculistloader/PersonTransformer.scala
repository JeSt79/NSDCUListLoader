/*
 * Copyright (c) 2018. Yevhen Stuzhnyi
 */

package ua.pp.yest.ndculistloader

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

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


object PersonTransformer {
  def props(personLoader: ActorRef): Props = Props(new PersonTransformer(personLoader))
}