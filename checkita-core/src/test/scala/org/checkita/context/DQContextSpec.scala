package org.checkita.context

import eu.timepit.refined.api.Refined
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types._
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.Common._
import org.checkita.config.Enums.SparkJoinType
import org.checkita.config.RefinedTypes.ID
import org.checkita.config.jobconf.Sources._
import org.checkita.core.Source
import org.checkita.utils.ResultUtils.{Result, liftToResult}

import scala.annotation.tailrec

class DQContextSpec extends AnyWordSpec with Matchers with PrivateMethodTester {

  private val context = DQContext.build(settings).getOrElse(new DQContext(settings, spark, fs))

  private val emptyDF = spark.createDataFrame(
    spark.sparkContext.emptyRDD[Row],
    StructType(Seq(
      StructField("c1", IntegerType, nullable = true),
      StructField("c2", StringType, nullable = true),
      StructField("c3", DoubleType, nullable = true)
    ))
  )
  "DQContext" must {
    "correctly read sequence of virtual sources" in {
      val vsReader = PrivateMethod[Result[Map[String, Source]]](Symbol("readVirtualSources"))
      val nextVsGetter = PrivateMethod[(VirtualSourceConfig, Seq[VirtualSourceConfig])](Symbol("getNextVS"))

      val parents = liftToResult(Seq(Source("emptyDF", emptyDF)).map(s => s.id -> s).toMap)
      val vs1 = FilterVirtualSourceConfig(
        ID("vs1"), None,
        Refined.unsafeApply(Seq("emptyDF")),
        Refined.unsafeApply(Seq(expr("c1 > 0"))),
        None, None, None
      )

      val vs2 = FilterVirtualSourceConfig(
        ID("vs2"), None,
        Refined.unsafeApply(Seq("emptyDF")),
        Refined.unsafeApply(Seq(expr("c2 is not null"))),
        None, None, None
      )

      val vs3 = JoinVirtualSourceConfig(
        ID("vs3"), None,
        Refined.unsafeApply(Seq("vs1", "vs2")),
        Refined.unsafeApply(Seq("c1")),
        SparkJoinType.Inner,
        None, None
      )

      val vs4 = AggregateVirtualSourceConfig(
        ID("vs4"), None,
        Refined.unsafeApply(Seq("vs3")),
        Refined.unsafeApply(Seq("c1")),
        Refined.unsafeApply(Seq(
          expr("sum(vs1.c3) as sum_c3"),
          expr("count(vs1.c1) as row_cnt"),
        )),
        None, None
      )

      val vs5 = SelectVirtualSourceConfig(
        ID("vs5"), None,
        Refined.unsafeApply(Seq("vs4")),
        Refined.unsafeApply(Seq(expr("sum_c3 / row_cnt as avg_value"))),
        None, None, None
      )

      val indices = Seq(0, 8, 16)
      val strPerm = Seq("vs1", "vs2", "vs3", "vs4", "vs5").permutations.toSeq
        .groupBy(_.head)
        .values
        .flatMap(sq => indices.map(idx => sq(idx)))
        .toSeq


      val vsSequences = Seq(vs1, vs2, vs3, vs4, vs5).permutations.toSeq
        .groupBy(_.head)
        .values
        .flatMap(sq => indices.map(idx => sq(idx)))
        .map(liftToResult).toSeq


      val orderResolving = vsSequences.map{ vsSeq =>
        @tailrec
        def readAll(vs: Seq[VirtualSourceConfig],
                    p: Map[String, Source],
                    acc: Seq[VirtualSourceConfig] = Seq.empty): Seq[VirtualSourceConfig] =
          if (vs.isEmpty) acc
          else {
            val (nextVs, restVs) = context invokePrivate nextVsGetter(vs, p, 0)
            val newP = p + (nextVs.id.value -> Source(nextVs.id.value, emptyDF))
            readAll(restVs, newP, acc :+ nextVs)
          }

        for {
          vs <- vsSeq
          p <- parents
        } yield readAll(vs, p)
      }

      // test how getNextVS function resolves virtual sources order:
      orderResolving.foreach { vsSeq =>
        vsSeq.isRight shouldEqual true
        val vs = vsSeq.getOrElse(Seq.empty)
        vs.head.parents shouldEqual Seq("emptyDF") // either vs1 or vs2
        vs.last.id.value shouldEqual "vs5"
      }

      // test how readVirtualSources function reads virtual sources:
      val results = vsSequences.map(vsSeq => context invokePrivate vsReader(vsSeq, parents, false, jobId))

      results.foreach { r =>
        r.isRight shouldEqual true

        val allSources = r.getOrElse(Map.empty)
        allSources.isEmpty shouldEqual false

        // test that all dataframes can be evaluated.
        allSources.values.foreach(src => src.df.collect().length shouldEqual 0)
      }
    }
  }
}
