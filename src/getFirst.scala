import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class getFirst extends UserDefinedAggregateFunction {
    // Schema you get as an input
    def inputSchema = new StructType().add("power", StringType)
    // Schema of the row which is used for aggregation
    def bufferSchema = new StructType().add("ind", StringType)
    // Returned type
    def dataType = StringType
    // Self-explaining 
    def deterministic = true
    // zero value
    def initialize(buffer: MutableAggregationBuffer) = buffer.update(0, "")
    // Similar to seqOp in aggregate
    def update(buffer: MutableAggregationBuffer, input: Row) = {
        if (!input.isNullAt(0))
          if(buffer.getString(0).equalsIgnoreCase(""))
          buffer.update(0, input.getString(0))
    }
    // Similar to combOp in aggregate
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
      if(buffer1.getString(0).equalsIgnoreCase(""))
      buffer1.update(0, buffer2.getString(0))    
    }
    // Called on exit to get return value
    def evaluate(buffer: Row) = buffer.getString(0)
}