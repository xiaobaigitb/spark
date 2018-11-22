/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
import scala.collection.mutable

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An example runner for logistic regression with elastic-net (mixing L1/L2) regularization.
 * Run with
 * {{{
 * bin/run-example ml.LogisticRegressionExample [options]
 * }}}
 * A synthetic dataset can be found at `data/mllib/sample_libsvm_data.txt` which can be
 * trained by
 * {{{
 * bin/run-example ml.LogisticRegressionExample --regParam 0.3 --elasticNetParam 0.8 \
 *   data/mllib/sample_libsvm_data.txt
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LogisticRegressionExample {


  def main(args: Array[String]) {
    run()

  }

  def loadData(spark: SparkSession): (DataFrame, DataFrame) = {
    import spark.implicits._
    val data = spark.sparkContext.textFile("d:\\test\\123.txt")
      .map(line => {
        val arr = line.split(" ")
        (arr(0).toInt, Vectors.dense(arr(1).toInt, arr(2).toInt))
      }).toDF("label", "features")
    val dfArr = data.randomSplit(Array(0.8, 0.2))
    (dfArr(0), dfArr(1))
  }

  def run(): Unit = {
    val spark = SparkSession
      .builder
        .master("local[*]")
      .appName(s"LogisticRegressionExample with ")
      .getOrCreate()

    println(s"LogisticRegressionExample with parameters:\n")

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = loadData(spark)
    // Set up Pipeline.

    val lor = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")




    val startTime = System.nanoTime()
    val lorModel = lor.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for logistic regression.
   // println(s"Weights: ${lorModel.coefficients} Intercept: ${lorModel.intercept}")

   // println("Training data results:")

    println("Test data results:")
    lorModel.transform(training).show()
    import  spark.implicits._
    val testData=spark.sparkContext.parallelize(List("160 80")).map(string=>{
      val arr=string.split(" ")
      (0,Vectors.dense(Array(arr(0).toDouble,arr(1).toDouble)))
    }).toDF("label","features")
    lorModel.transform(testData).show()
    spark.stop()
  }
}
// scalastyle:on println
