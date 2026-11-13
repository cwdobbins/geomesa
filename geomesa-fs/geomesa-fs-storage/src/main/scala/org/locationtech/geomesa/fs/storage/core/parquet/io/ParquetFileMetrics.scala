/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.parquet.io

import org.apache.iceberg.{FieldMetrics, Metrics, MetricsConfig, TableProperties}
import org.apache.iceberg.io.InputFile
import org.apache.iceberg.parquet.{ParquetSchemaUtil, ParquetUtil}
import org.apache.iceberg.types.TypeUtil
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.{DelegatingSeekableInputStream, InputFile => ParquetInputFile, SeekableInputStream}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Type}
import org.locationtech.geomesa.utils.io.WithClose

import java.util.stream.Stream

/** Conservative Iceberg metrics for files whose VARIANT binary statistics may be absent. */
object ParquetFileMetrics {

  import scala.collection.JavaConverters._

  def fileMetrics(file: InputFile, config: MetricsConfig): Metrics = {
    val input = new ParquetInputFile {
      override def getLength: Long = file.getLength
      override def newStream(): SeekableInputStream = {
        val stream = file.newStream()
        new DelegatingSeekableInputStream(stream) {
          override def getPos: Long = stream.getPos
          override def seek(position: Long): Unit = stream.seek(position)
        }
      }
    }
    WithClose(ParquetFileReader.open(input)) { reader =>
      val footer = reader.getFooter
      val original = footer.getFileMetaData.getSchema
      val schema = if (ParquetSchemaUtil.hasIds(original)) { original } else {
        ParquetSchemaUtil.addFallbackIds(original)
      }
      val incomplete = scala.collection.mutable.Set.empty[Int]
      val blocks = footer.getBlocks.asScala.map { block =>
        block.getColumns.asScala.map(c => c.getPath.toArray.toVector -> c).toMap
      }

      def requiredCounts(field: Type, path: Vector[String]): Vector[Vector[String]] = {
        if (field.isPrimitive) {
          if (field.getName == "metadata" || field.getName == "value") { Vector(path) } else { Vector.empty }
        } else {
          field.asGroupType().getFields.asScala.toVector.flatMap(f => requiredCounts(f, path :+ f.getName))
        }
      }
      def inspect(field: Type, path: Vector[String]): Unit = {
        if (field.getLogicalTypeAnnotation.isInstanceOf[LogicalTypeAnnotation.VariantLogicalTypeAnnotation]) {
          val required = requiredCounts(field, path)
          val missing = blocks.exists { columns =>
            required.exists { key =>
              val column = columns.getOrElse(key,
                throw new IllegalArgumentException(s"Missing Parquet column $key in ${file.location()}"))
              val stats = column.getStatistics
              stats == null || stats.isEmpty || !stats.isNumNullsSet
            }
          }
          if (missing && field.getId != null) { incomplete += field.getId.intValue() }
        } else if (!field.isPrimitive) {
          field.asGroupType().getFields.asScala.foreach(f => inspect(f, path :+ f.getName))
        }
      }
      schema.getFields.asScala.foreach(f => inspect(f, Vector(f.getName)))
      val effective = if (incomplete.isEmpty) { config } else {
        // Iceberg 1.11 assumes serialized VARIANT counts exist, and can throw or mistake a typed
        // child's counts for the root's when they do not. Disable only affected VARIANT metrics.
        // Preserve every other column's requested mode, including explicit none/counts/truncation.
        val iceberg = ParquetSchemaUtil.convertAndPrune(schema)
        val properties = TypeUtil.indexNameById(iceberg.asStruct()).asScala.map { case (id, name) =>
          val mode = if (incomplete.contains(id.intValue())) { "none" } else { config.columnMode(name).toString }
          (TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + name) -> mode
        }
        MetricsConfig.fromProperties(properties.asJava)
      }
      ParquetUtil.footerMetrics(footer, Stream.empty[FieldMetrics[_]](), effective)
    }
  }
}
