/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.mango.core.util
import java.io.Serializable

import org.bdgenomics.formats.avro.{ GenotypeAllele, Variant }
import ga4gh.Variants.Variant

import scala.util.parsing.json.JSONFormat
//import org.apache.spark.SparkContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark._
import org.apache.spark.rdd.RDD

object GA4GHUtil {
  def main(args: Array[String]): Unit = {
    println("Hello Justin")

    val x_builder = ga4gh.Variants.Variant.newBuilder()
    x_builder.setEnd(342424)
    val x = x_builder.build()

    val x2_builder = ga4gh.Variants.Variant.newBuilder()
    x2_builder.setEnd(342424)
    val x2 = x2_builder.build()

    //if(Serializable.class.isInstanceOf[x]

    val myString = com.google.protobuf.util.JsonFormat.printer().print(x)
    println("myString: " + myString)

    val z_builder = ga4gh.Variants.Variant.newBuilder()
    com.google.protobuf.util.JsonFormat.parser().merge(myString, z_builder)

    val z2 = z_builder.build()
    println("This is z_builder: " + z2)
    println("This is its string of z2 :" + com.google.protobuf.util.JsonFormat.printer().print(z2))

    //    val sc = new SparkContext()

    /*
    val conf = new SparkConf()
      .setAppName("eQTL linear regression App using ADAM formats")
    val sc = new SparkContext(conf)

    val l: Seq[ga4gh.Variants.Variant] = List(x, x2)

    val z: RDD[ga4gh.Variants.Variant] = sc.parallelize(l).repartition(2)
    val z1 = z.collect()
    println("rdd_count r: " + z.count())
    println("rdd num of paritions: " + z.getNumPartitions)

    val p = z.coalesce(1, shuffle = true)
    val o = p.count()

    println(z1)

*/

  }
  def GA4GHVariantToBDG(x: ga4gh.Variants.Variant): org.bdgenomics.formats.avro.Variant = {
    val bdgVariantBuilder = org.bdgenomics.formats.avro.Variant.newBuilder()
    bdgVariantBuilder.setAlternateAllele(x.getAlternateBases(0))
    bdgVariantBuilder.setReferenceAllele(x.getReferenceBases)
    bdgVariantBuilder.setStart(x.getStart)
    bdgVariantBuilder.setEnd(x.getEnd)
    bdgVariantBuilder.setContigName(x.getReferenceName)
    bdgVariantBuilder.build()
  }

}

