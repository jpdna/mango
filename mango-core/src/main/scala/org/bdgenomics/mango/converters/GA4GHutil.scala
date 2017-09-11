package org.bdgenomics.mango.converters

import javax.inject.Inject

import com.google.inject._
import ga4gh.Reads.ReadAlignment
import net.codingwell.scalaguice.ScalaModule
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.convert.ga4gh.Ga4ghModule
import org.bdgenomics.convert.{ ConversionStringency, Converter }
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.mango.converters
import org.bdgenomics.mango.converters.AlignmentRecordConverterGA4GH.bdgToGA4GH

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by paschalj on 9/8/17.
 */

object GA4GHutil {
  val injector: Injector = Guice.createInjector(new Ga4ghModule())
  val converter: Converter[AlignmentRecord, ReadAlignment] = injector.getInstance(Key.get(new TypeLiteral[Converter[AlignmentRecord, ReadAlignment]]() {}))
  def alignmentRecordRDDtoJSON(alignmentRecordRDD: AlignmentRecordRDD): String = {
    val x: Array[AlignmentRecord] = alignmentRecordRDD.rdd.collect()

    val logger = LoggerFactory.getLogger("GA4GHutil")

    val gaReads = x.map(a => converter.convert(a, ConversionStringency.LENIENT, logger))

    val result: ga4gh.ReadServiceOuterClass.SearchReadsResponse = ga4gh.ReadServiceOuterClass.SearchReadsResponse.newBuilder().addAllAlignments(gaReads.toList.asJava).build()
    com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(result)
  }
}
