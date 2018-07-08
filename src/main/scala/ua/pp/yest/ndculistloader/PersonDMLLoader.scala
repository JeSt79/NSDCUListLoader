package ua.pp.yest.ndculistloader

import akka.actor.{Actor, ActorLogging, Props}

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

object PersonDMLLoader{
  def props(scriptFileName: String): Props = Props(new PersonDMLLoader(scriptFileName))
}
