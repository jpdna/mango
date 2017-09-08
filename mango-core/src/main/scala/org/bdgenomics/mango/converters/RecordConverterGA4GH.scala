package org.bdgenomics.mango.converters

import java.lang.Boolean
import java.util

import org.bdgenomics.utils.misc.Logging

import scala.reflect.ClassTag

/**
 * Created by paschalj on 6/20/17.
 */
abstract class RecordConverterGA4GH[T: ClassTag, S: ClassTag] extends Serializable with Logging {

  def bdgToGA4GH(record: T): S

  def ga4ghSeqtoJSON(gaReads: Seq[S]): String

}
