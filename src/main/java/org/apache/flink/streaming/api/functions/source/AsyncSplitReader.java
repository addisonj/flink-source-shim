package org.apache.flink.streaming.api.functions.source;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface AsyncSplitReader<OUT> extends Closeable {

  /**
   * Asynchronously fetches a single record
   *
   * <p>For fetching batches, {@link AsyncSplitReader#fetchNextRecords()} may be overridden instead
   *
   * @return a record or null
   * @throws IOException
   */
  CompletableFuture<OUT> fetchNextRecord() throws IOException;

  /**
   * Returns a group of records, if no records can be returned within the duration return an empty
   * list
   *
   * @return
   * @throws IOException
   */
  default CompletableFuture<List<OUT>> fetchNextRecords() throws IOException {
    return fetchNextRecord().thenApply((r) -> Collections.singletonList(r));
  }

  /**
   * If you want to support extracting timestamps, override this method
   *
   * @param record
   * @return
   */
  @Nullable
  default Long extractTimestamp(OUT record) {
    return null;
  }
}
