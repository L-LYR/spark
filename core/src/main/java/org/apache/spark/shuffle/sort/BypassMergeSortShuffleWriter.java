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

package org.apache.spark.shuffle.sort;

import java.io.*;
import java.util.ArrayList;
import javax.annotation.Nullable;

import org.apache.spark.*;
import org.apache.spark.serializer.SerializationStream;
import org.apache.spark.util.collection.PartitionedAppendOnlyMap;
import org.apache.spark.util.collection.WritablePartitionedIterator;
import org.glassfish.jersey.internal.util.collection.KeyComparator;
import pdsl.dpx.type.TypeTraits;
import scala.*;
import scala.Boolean;
import scala.collection.Iterator;
import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.InTaskMetrics;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.scheduler.MapStatus$;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.storage.*;
import org.apache.spark.util.Utils;
import scala.math.Ordering;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import pdsl.dpx.TransEnv;
import pdsl.dpx.Serde;

/**
 * This class implements sort-based shuffle's hash-style shuffle fallback path. This write path
 * writes incoming records to separate files, one file per reduce partition, then concatenates these
 * per-partition files to form a single output file, regions of which are served to reducers.
 * Records are not buffered in memory. It writes output in a format
 * that can be served / consumed via {@link org.apache.spark.shuffle.IndexShuffleBlockResolver}.
 * <p>
 * This write path is inefficient for shuffles with large numbers of reduce partitions because it
 * simultaneously opens separate serializers and file streams for all partitions. As a result,
 * {@link SortShuffleManager} only selects this write path when
 * <ul>
 *    <li>no Ordering is specified,</li>
 *    <li>no Aggregator is specified, and</li>
 *    <li>the number of partitions is less than
 *      <code>spark.shuffle.sort.bypassMergeThreshold</code>.</li>
 * </ul>
 *
 * This code used to be part of {@link org.apache.spark.util.collection.ExternalSorter} but was
 * refactored into its own class in order to reduce code complexity; see SPARK-7855 for details.
 * <p>
 * There have been proposals to completely remove this code path; see SPARK-6026 for details.
 */
final class BypassMergeSortShuffleWriter<K, V, C> extends ShuffleWriter<K, V> {

  private static final ClassTag<Object> OBJECT_CLASS_TAG = ClassTag$.MODULE$.Object();

  private static final Logger logger = LoggerFactory.getLogger(BypassMergeSortShuffleWriter.class);

  private final int fileBufferSize;
  private final boolean transferToEnabled;
  private final int numPartitions;
  private final BlockManager blockManager;
  private final Partitioner partitioner;
  private final ShuffleWriteMetrics writeMetrics;
  private final InTaskMetrics inTaskMetrics;
  private final int shuffleId;
  private final Option<Aggregator<K, V, C>> aggregator;
  private final Option<Ordering<K>> ordering;
  private final int mapId;
  private final Serializer serializer;
  private final IndexShuffleBlockResolver shuffleBlockResolver;

  /** Array of file writers, one for each partition */
  private DiskBlockObjectWriter[] partitionWriters;
  private FileSegment[] partitionWriterSegments;
  @Nullable private MapStatus mapStatus;
  private long[] partitionLengths;

  /**
   * Are we in the process of stopping? Because map tasks can call stop() with success = true
   * and then call stop() with success = false if they get an exception, we want to make sure
   * we don't try deleting files, etc twice.
   */
  private boolean stopping = false;

  BypassMergeSortShuffleWriter(
      BlockManager blockManager,
      IndexShuffleBlockResolver shuffleBlockResolver,
      BypassMergeSortShuffleHandle<K, V, C> handle,
      int mapId,
      TaskContext taskContext,
      SparkConf conf) {
    logger.info("Use BypassMergeSortShuffleWriter");
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024;
    this.transferToEnabled = conf.getBoolean("spark.file.transferTo", true);
    this.blockManager = blockManager;
    final ShuffleDependency<K, V, C> dep = handle.dependency();
    this.aggregator = dep.aggregator();
    this.ordering = dep.keyOrdering();
    this.mapId = mapId;
    this.shuffleId = dep.shuffleId();
    this.partitioner = dep.partitioner();
    this.numPartitions = partitioner.numPartitions();
    this.writeMetrics = taskContext.taskMetrics().shuffleWriteMetrics();
    this.inTaskMetrics =taskContext.taskMetrics().inTaskMetrics();
    this.serializer = dep.serializer();
    logger.info(this.serializer.getClass().getName());
    this.shuffleBlockResolver = shuffleBlockResolver;
  }

  @Override
  public void write(Iterator<Product2<K, V>> records) throws IOException {
    assert (partitionWriters == null);
    if (!records.hasNext()) {
      partitionLengths = new long[numPartitions];
      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, null);
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }
    TransEnv.TriggerSpillStart();
//    final long openStartTime = System.nanoTime();
//    partitionWriters = new DiskBlockObjectWriter[numPartitions];
//    partitionWriterSegments = new FileSegment[numPartitions];
//    for (int i = 0; i < numPartitions; i++) {
//      final Tuple2<TempShuffleBlockId, File> tempShuffleBlockIdPlusFile =
//        blockManager.diskBlockManager().createTempShuffleBlock();
//      final File file = tempShuffleBlockIdPlusFile._2();
//      final BlockId blockId = tempShuffleBlockIdPlusFile._1();
////      logger.info("BlockId {}", blockId.name());
//      partitionWriters[i] =
//        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
//    }
    // Creating the file to write to and creating a disk writer both involve interacting with
    // the disk, and can take a long time in aggregate when we open many files, so should be
    // included in the shuffle write time.
//    writeMetrics.incWriteTime(System.nanoTime() - openStartTime);
    boolean print_type = true;
    long[] partitionLengths = new long[numPartitions];
    Serde sd = new Serde();
    if (aggregator.isDefined()) {
     logger.info("has aggregator");
      Aggregator<K,V,C> agg = aggregator.get();
      PartitionedAppendOnlyMap<K, C> m = new PartitionedAppendOnlyMap<K, C>();
      Product2<K, V> kv = null;
      while(records.hasNext()) {
        final Product2<K, V> record = records.next();
        final K key = record._1();
        final V value = record._2();
        if (print_type) {
            if (key == null) {
                logger.info("K is null");
            } else {
                logger.info("K is {}", key.getClass().getName());
            }
            if (value == null) {
                logger.info("V is null");
            } else {
                logger.info("V is {}", value.getClass().getName());
            }
            print_type = false;
        }
        int p = partitioner.getPartition(key);
        final Tuple2<Object, K> ck = Tuple2.apply(p, key);
        C cur = m.apply(ck);
        if (cur != null) {
            m.update(ck, agg.mergeValue().apply(cur, value));
        } else {
            m.update(ck, agg.createCombiner().apply(value));
        }
        if (m.size() > 10000) {
            Comparator<K> d = null;
            if (ordering.isDefined()) {
                d = ordering.get();
            } else {
                d = new Comparator<K>() {
                    @Override
                    public int compare(K a, K b) {
                        int h1 = 0;
                        if (a != null) {
                            h1 = a.hashCode();
                        }
                        int h2 = 0;
                        if (b != null) {
                            h2 = a.hashCode();
                        }
                        if (h1 < h2) {
                            return -1;
                        }
                        if (h1 == h2) {
                            return 0;
                        }
                        return 1;
                    }
                };
            }

            Iterator<Tuple2<Tuple2<Object, K>, C>> it
                = m.partitionedDestructiveSortedIterator(new Some<>(d));
            while(it.hasNext()) {
                Tuple2<Tuple2<Object, K>, C> r = it.next();
                Integer pid = (Integer) r._1._1;
                final K k = record._1();
                final V v = record._2();
                final long spillSerializeStart = System.nanoTime();
                final byte[] ks = sd.Serialize(key);
                final byte[] vs = sd.Serialize(value);
                inTaskMetrics.incSerializeTime(System.nanoTime() - spillSerializeStart);

                final long spillShuffleStart = System.nanoTime();
                TransEnv.Append(p, ks, vs, !records.hasNext());
                inTaskMetrics.incSpillTime(System.nanoTime() - spillShuffleStart);
                partitionLengths[p] += ks.length + vs.length;
            }

            m = new PartitionedAppendOnlyMap<>();
        }
      }
      mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
      return;
    }

//    final BlockId blockId = new TestBlockId("test" + Integer.toString(mapId));
//    SerializationStream ss = serInstance.serializeStream(bs);
//    ArrayList<Integer> offsets = new ArrayList<>();
//    ArrayList<Integer> hashcodes = new ArrayList<>();

//    final File file = new File("/home/lsc/dpx/.test_spill/t" + Integer.toString(mapId));
//    final FileOutputStream fos = new FileOutputStream(file);
//    final BufferedOutputStream bos = new BufferedOutputStream(fos);
//    final SerdeOutputStream sos2 = new SerdeOutputStream(bos);
////      logger.info("BlockId {}", blockId.name());
//    final DiskBlockObjectWriter w =
//        blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, writeMetrics);
    while (records.hasNext()) {
      final Product2<K, V> record = records.next();
      final K key = record._1();
      final V value = record._2();
        if (print_type) {
            if (key == null) {
                logger.info("K is null");
            } else {
                logger.info("K is {}", key.getClass().getName());
            }
            if (value == null) {
                logger.info("V is null");
            } else {
                logger.info("V is {}", value.getClass().getName());
            }
            print_type = false;
        }
//      partitionWriters[partitioner.getPartition(key)].write(key, record._2());
      int p = partitioner.getPartition(key);
//      offsets.add(bs.size());
//      partitionLengths[p] += bs.size() - offsets.get(offsets.size() - 1);
      final long spillSerializeStart = System.nanoTime();
      final byte[] k = sd.Serialize(key);
      final byte[] v = sd.Serialize(value);
      inTaskMetrics.incSerializeTime(System.nanoTime() - spillSerializeStart);

      final long spillShuffleStart = System.nanoTime();
      TransEnv.Append(p, k, v, !records.hasNext());
      inTaskMetrics.incSpillTime(System.nanoTime() - spillShuffleStart);
      partitionLengths[p] += k.length + v.length;
//      hashcodes.add(p);

//      sos2.writeObject(key);
//      sos2.writeObject(value);

//      w.write(key, value);
    }
//    offsets.add(bs.size());

//    logger.info("{} records", hashcodes.size());
//    w.commitAndGet();
//    logger.info("{} {} {} {}", result[0], result[1], result[2], result[3]);

//    sos2.flush();
//    sos2.close();

//    for (int i = 0; i < numPartitions; i++) {
//      final DiskBlockObjectWriter writer = partitionWriters[i];
//      partitionWriterSegments[i] = writer.commitAndGet();
//      writer.close();
//    }

//    File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
//    logger.info("output file: {}", output.getPath());
//    File tmp = Utils.tempFileWith(output);
//    try {
//      partitionLengths = writePartitionedFile(tmp);
//      shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
//    } finally {
//      if (tmp.exists() && !tmp.delete()) {
//        logger.error("Error while deleting temp file {}", tmp.getAbsolutePath());
//      }
//    }
    mapStatus = MapStatus$.MODULE$.apply(blockManager.shuffleServerId(), partitionLengths);
  }

  @VisibleForTesting
  long[] getPartitionLengths() {
    return partitionLengths;
  }

  /**
   * Concatenate all of the per-partition files into a single combined file.
   *
   * @return array of lengths, in bytes, of each partition of the file (used by map output tracker).
   */
  private long[] writePartitionedFile(File outputFile) throws IOException {
    // Track location of the partition starts in the output file
    final long[] lengths = new long[numPartitions];
    if (partitionWriters == null) {
      // We were passed an empty iterator
      return lengths;
    }

    final FileOutputStream out = new FileOutputStream(outputFile, true);
    final long writeStartTime = System.nanoTime();
    boolean threwException = true;
    try {
      for (int i = 0; i < numPartitions; i++) {
        final File file = partitionWriterSegments[i].file();
        if (file.exists()) {
          final FileInputStream in = new FileInputStream(file);
          boolean copyThrewException = true;
          try {
            lengths[i] = Utils.copyStream(in, out, false, transferToEnabled);
            copyThrewException = false;
          } finally {
            Closeables.close(in, copyThrewException);
          }
          if (!file.delete()) {
            logger.error("Unable to delete file for partition {}", i);
          }
        }
      }
      threwException = false;
    } finally {
      Closeables.close(out, threwException);
      writeMetrics.incWriteTime(System.nanoTime() - writeStartTime);
    }
    partitionWriters = null;
    return lengths;
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (stopping) {
      return None$.empty();
    } else {
      stopping = true;
      if (success) {
        if (mapStatus == null) {
          throw new IllegalStateException("Cannot call stop(true) without having called write()");
        }
        return Option.apply(mapStatus);
      } else {
        // The map task failed, so delete our output data.
        if (partitionWriters != null) {
          try {
            for (DiskBlockObjectWriter writer : partitionWriters) {
              // This method explicitly does _not_ throw exceptions:
              File file = writer.revertPartialWritesAndClose();
              if (!file.delete()) {
                logger.error("Error while deleting file {}", file.getAbsolutePath());
              }
            }
          } finally {
            partitionWriters = null;
          }
        }
        return None$.empty();
      }
    }
  }
}
