package Ml

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable
import smile.data.formula.Formula
import smile.data.formula.FormulaBuilder
import smile.data.formula.Term

class GroupedByUDAF(x: Array[String], y: String) extends UserDefinedAggregateFunction {
  //Buffer to save the models
  private var modelsBuffer: mutable.ArrayBuffer[smile.regression.LinearModel] = _

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = {
    StructType(x.map(col => StructField(col, DoubleType)))
  }

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("prediction", DoubleType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array.empty[Array[Any]]
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer(0).asInstanceOf[Array[Array[Any]]] :+ input.toSeq.toArray
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.asInstanceOf[Array[Array[Any]]] ++ buffer2.toSeq.toArray.asInstanceOf[Array[Array[Any]]]
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val form = Formula.lhs(y)
    val data = smile.data.DataFrame.of(buffer.asInstanceOf[Array[Array[Double]]])
    smile.regression.RidgeRegression.fit(form, data, 0.0057)
  }
}