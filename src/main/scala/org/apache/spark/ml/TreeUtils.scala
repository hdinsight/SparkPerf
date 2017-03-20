/*
 * Modifications copyright (C) 2017 Microsoft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml

import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.sql.DataFrame

object TreeUtils {
  /**
   * Set label metadata (particularly the number of classes) on a DataFrame.
   *
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param featuresColName  Name of the features column
   * @param featureArity  Array of length numFeatures, where 0 indicates continuous feature and
   *                      value > 0 indicates a categorical feature of that arity.
   * @return  DataFrame with metadata
   */
  def setMetadata(
      data: DataFrame,
      featuresColName: String,
      featureArity: Array[Int]): DataFrame = {
    val featuresAttributes = featureArity.zipWithIndex.map { case (arity: Int, feature: Int) =>
      if (arity > 0) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(arity)
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    data.select(data(featuresColName).as(featuresColName, featuresMetadata))
  }
}
