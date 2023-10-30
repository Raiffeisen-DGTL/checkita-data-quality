package ru.raiffeisen.checkita.core.checks.load

import org.apache.spark.sql.types.{StructField, StructType}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.core.Results.LoadCheckCalculatorResult
import ru.raiffeisen.checkita.core.checks.{LoadCheckCalculator, LoadCheckName}
import ru.raiffeisen.checkita.core.{CalculatorStatus, Source}
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema

import scala.annotation.tailrec

/**
 * `Schema match` load check calculator:
 * verifies if source schema matches required schema
 * (defined in schemas section of job configuration and referenced by its ID)
 *
 * @param checkId Load check ID
 * @param schemaId Sequence of required columns
 */
final case class SchemaMatchLoadCheckCalculator(
                                                 checkId: String,
                                                 schemaId: String,
                                                 ignoreOrder: Boolean
                                               ) extends LoadCheckCalculator {

  val checkName: LoadCheckName = LoadCheckName.SchemaMatch
  val expected: String = s"Schema($schemaId)"
  val detailsMsg: String = s"source schema matches schema definition with id '$schemaId'."

  /**
   * Runs load check for the given source.
   * @param source Source to check
   * @param schemas Map of predefined source schemas
   * @param settings Implicit application settings
   * @return Load check evaluation result with either Success or Failure status
   */
  def tryToRun(source: Source,
               schemas: Map[String, SourceSchema] = Map.empty)
              (implicit settings: AppSettings): LoadCheckCalculatorResult = {
    
    val targetSchema = schemas.getOrElse(schemaId, throw new NoSuchElementException(
      s"Schema with ID '$schemaId' not found."
    ))
    implicit val isOrdered: Boolean = !ignoreOrder
    
    val status = if (source.df.schema ~~ targetSchema.schema) CalculatorStatus.Success else CalculatorStatus.Failure
    val statusMsg =
      if (status == CalculatorStatus.Success) "Source schema matches target schema."
      else s"Source schema DOES NOT match target schema."

    LoadCheckCalculatorResult(
      checkId,
      checkName.entryName,
      source.id,
      expected,
      status,
      getMessage(source.id, status, statusMsg)
    )
  }

  /**
   * Implicit string comparator: compares to strings depending 
   * on the application case sensitivity option
   * @param s1 Base string
   * @param settings Implicit application settings
   */
  private implicit class StringComparator(s1: String)(implicit settings: AppSettings) {
    /**
     * Compares two strings
     * @param s2 Compare string
     * @return String comparison result
     */
    def --(s2: String): Boolean = 
      if (settings.enableCaseSensitivity) s1 == s2
      else s1.toLowerCase == s2.toLowerCase
  }

  /**
   * Implicit schema comparator that supports two methods of schema comparison:
   *   - with columns order being taken into account
   *   - with columns order being ignored
   * Also takes into account application case sensitivity option when comparing column names.
   * @param s1 Base schema
   * @param settings Implicit application settings
   * @param isOrdered Boolean flag to chose schema comparison method.
   */
  private implicit class SchemaComparator(s1: StructType)
                                         (implicit settings: AppSettings, isOrdered: Boolean) {

    /**
     * Compares two schemas.
     * @param s2 Compare schema
     * @return Boolean schema comparison result
     */
    def ~~(s2: StructType): Boolean = {
      
      val lvlFieldEqual: ((Int, StructField), (Int, StructField)) => Boolean =
        (f1, f2) => f1._1 == f2._1 && f1._2.name -- f2._2.name && f1._2.dataType == f2._2.dataType
      val lvlFieldEqualStruct: ((Int, StructField), (Int, StructField)) => Boolean =
        (f1, f2) => f1._1 == f2._1 && f1._2.name -- f2._2.name && 
          f1._2.dataType.isInstanceOf[StructType] && f2._2.dataType.isInstanceOf[StructType]

      /**
       * Schema comparison with respect to columns order.
       */
      @tailrec
      def isEqualOrdered(sq1: Seq[StructField], sq2: Seq[StructField]): Boolean =
        (sq1, sq2) match {
          case (Nil, Nil) => true
          case (f1 :: t1, f2 :: t2) if f1.name -- f2.name => (f1.dataType, f2.dataType) match {
            case (d1: StructType, d2: StructType) => isEqualOrdered(d1.toList ++ t1, d2.toList ++ t2)
            case (d1, d2) if d1 == d2 => isEqualOrdered(t1, t2)
            case _ => false
          }
          case _ => false
        }

      /**
       * Schema comparison ignoring columns order.
       */
      @tailrec
      def isEqualUnordered(sq1: Seq[(Int, StructField)], sq2: Seq[(Int, StructField)]): Boolean = {
        (sq1, sq2) match {
          case (Nil, Nil) => true
          case (f1 :: t1, _ :: _) if f1._2.dataType.isInstanceOf[StructType] && 
            sq2.exists(t => lvlFieldEqualStruct(f1, t)) =>
            // increment lvl of first schema subfields:
            val ff1 = f1._2.dataType.asInstanceOf[StructType].toList.map(f => (f1._1 + 1, f))
            // second schema subfield with incremented lvl:
            val ff2 = sq2.filter(t => lvlFieldEqualStruct(f1, t))
              .flatMap(t => t._2.dataType.asInstanceOf[StructType].toList.map(f => (t._1 + 1, f)))
            // second schema tail:
            val t2 = sq2.filterNot(t => lvlFieldEqualStruct(f1, t))
            
            // compare names of these fields and add subfields with incremented level:
            isEqualUnordered(ff1 ++ t1, ff2 ++ t2) 
          case (f1 :: t1, _ :: _) if sq2.exists(f => lvlFieldEqual(f1, f)) =>
            val t2 = sq2.filterNot(f => lvlFieldEqual(f1, f))
            isEqualUnordered(t1, t2)
          case _ => false
        }
      }
      
      if (isOrdered) isEqualOrdered(s1.toList, s2.toList) else {
        isEqualUnordered(s1.toList.map(f => (1, f)), s2.toList.map(f => (1, f)))
      }
    }
  }
  
}
