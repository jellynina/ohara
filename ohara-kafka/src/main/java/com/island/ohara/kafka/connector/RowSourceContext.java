package com.island.ohara.kafka.connector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceTaskContext;

/** a wrap to kafka SourceTaskContext */
public interface RowSourceContext {

  /**
   * Get the offset for the specified partition. If the data isn't already available locally, this
   * gets it from the backing store, which may require some network round trips.
   *
   * @param partition object uniquely identifying the partition from data
   * @return object uniquely identifying the offset in the partition from data
   */
  <T> Map<String, Object> offset(Map<String, T> partition);

  /**
   * Get a set from offsets for the specified partition identifiers. This may be more efficient than
   * calling offset() repeatedly.
   *
   * <p>Note that when errors occur, this method omits the associated data and tries to return as
   * many from the requested values as possible. This allows a task that's managing many partitions
   * to still proceed with any available data. Therefore, implementations should take care to check
   * that the data is actually available in the returned response. The only case when an exception
   * will be thrown is if the entire request failed, e.g. because the underlying storage was
   * unavailable.
   *
   * @param partitions set from identifiers for partitions from data
   * @return a map from partition identifiers to decoded offsets
   */
  <T> Map<Map<String, T>, Map<String, Object>> offset(List<Map<String, T>> partitions);

  static RowSourceContext toRowSourceContext(SourceTaskContext context) {
    return new RowSourceContext() {

      @Override
      public <T> Map<String, Object> offset(Map<String, T> partition) {

        Map<String, Object> r = context.offsetStorageReader().offset(partition);
        return r == null ? Collections.emptyMap() : r;
      }

      @Override
      public <T> Map<Map<String, T>, Map<String, Object>> offset(List<Map<String, T>> partitions) {

        return context.offsetStorageReader().offsets(partitions);
      }
    };
  }
}