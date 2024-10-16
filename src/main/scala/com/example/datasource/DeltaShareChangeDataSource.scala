package com.example.datasource

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming._
import org.apache.spark.sql.sources.v2.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Optional
import scala.collection.JavaConverters._
import java.time.OffsetDateTime

class DeltaShareChangeDataSource extends TableProvider with DataSourceOptions {

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DeltaShareChangeTable(options)
  }

  override def shortName(): String = "delta_share_cdf"
}

class DeltaShareChangeTable(options: CaseInsensitiveStringMap) extends Table with SupportsRead {

  override def name(): String = "DeltaShareChangeTable"

  // Infer the schema from the table
  override def schema(): StructType = {
    val spark = SparkSession.active
    val tablePath = options.get("tablePath")
    val fromLatest = options.getOrDefault("fromLatest", "false").toBoolean
    val table = spark.read.format("delta").load(tablePath)
    val originalSchema = table.schema

    // Define the additional CDF columns
    val cdfColumns = StructType(Seq(
      StructField("_change_type", StringType, true),
      StructField("_commit_version", LongType, true),
      StructField("_commit_timestamp", TimestampType, true)
    ))

    // Combine the original schema with the CDF columns
    StructType(originalSchema.fields ++ cdfColumns.fields)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new DeltaShareChangeScanBuilder(options, schema())
  }
}

class DeltaShareChangeScanBuilder(options: CaseInsensitiveStringMap, schema: StructType)
  extends ScanBuilder with SupportsMicroBatchRead {

  override def build(): Scan = {
    new DeltaShareChangeScan(options, schema)
  }

  override def newMicroBatchReader(
    schema: Optional[StructType],
    checkpointLocation: String
  ): MicroBatchReader = {
    new DeltaShareChangeMicroBatchReader(schema.orElse(this.schema), options.asCaseSensitiveMap(), checkpointLocation)
  }
}

class DeltaShareChangeScan(options: CaseInsensitiveStringMap, schema: StructType) extends Scan {
  override def readSchema(): StructType = schema

  override def toBatch: Batch = {
    throw new UnsupportedOperationException("Batch read is not supported.")
  }
}

class DeltaShareChangeMicroBatchReader(
    val schema: StructType,
    val options: java.util.Map[String, String],
    val checkpointLocation: String
) extends MicroBatchReader {

  private var startOffset: Offset = _
  private var endOffset: Offset = _

  private val tablePath = options.get("tablePath")
  if (tablePath == null) {
    throw new IllegalArgumentException("Option 'tablePath' must be specified.")
  }

  private val fromLatest = options.getOrDefault("fromLatest", "false").toBoolean

  private val numPartitions = options.getOrDefault("numPartitions", "1").toInt

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {
    val spark = SparkSession.active
    val deltaLog = io.delta.tables.DeltaTable.forPath(spark, tablePath).toDF()

    val latestVersion = deltaLog.selectExpr("max(_commit_version) as max_version")
      .collect()(0).getAs[Long]("max_version")

    startOffset = start.orElse {
      if (fromLatest) LongOffset(latestVersion)
      else LongOffset(0L)
    }

    endOffset = end.orElse(LongOffset(latestVersion))
  }

  override def getStartOffset: Offset = startOffset

  override def getEndOffset: Offset = endOffset

  override def planInputPartitions(): java.util.List[InputPartition[InternalRow]] = {
    val spark = SparkSession.active

    val startVersion = startOffset.asInstanceOf[LongOffset].offset
    val endVersion = endOffset.asInstanceOf[LongOffset].offset

    val versionsPerPartition = Math.max(1, (endVersion - startVersion + 1) / numPartitions)
    val versionRanges = (startVersion to endVersion by versionsPerPartition).sliding(2).map {
      case Seq(start, end) => (start, Math.min(end - 1, endVersion))
    }.toList

    versionRanges.map { case (vStart, vEnd) =>
      new DeltaShareChangeInputPartition(tablePath, vStart, vEnd, options)
    }.asInstanceOf[List[InputPartition[InternalRow]]].asJava
  }

  override def readSchema(): StructType = schema

  override def stop(): Unit = {}

  override def commit(end: Offset): Unit = {}

  override def needsReconfiguration(): Boolean = false
}

class DeltaShareChangeInputPartition(
    val tablePath: String,
    val startVersion: Long,
    val endVersion: Long,
    val options: java.util.Map[String, String]
) extends InputPartition[InternalRow] {

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new DeltaShareChangeInputPartitionReader(tablePath, startVersion, endVersion)
  }
}

class DeltaShareChangeInputPartitionReader(
    val tablePath: String,
    val startVersion: Long,
    val endVersion: Long
) extends InputPartitionReader[InternalRow] {

  private val spark = SparkSession.active

  private val iterator: Iterator[InternalRow] = {
    val df = spark.sql(s"SELECT * FROM table_changes('$tablePath', $startVersion, $endVersion)")
    df.queryExecution.toRdd.mapPartitions { rows =>
      rows.map { row =>
        row.asInstanceOf[InternalRow]
      }
    }.toLocalIterator.asScala
  }

  override def next(): Boolean = iterator.hasNext

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {}
}

// Helper class for Offset
case class LongOffset(offset: Long) extends Offset {
  override def json(): String = offset.toString
}
