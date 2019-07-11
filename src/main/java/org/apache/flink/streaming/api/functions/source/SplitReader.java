package org.apache.flink.streaming.api.functions.source;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

public interface SplitReader<OUT> extends Closeable {

  /**
   * Fetches a single record, may return null if no records are available
   * within the duration
   *
   * For fetching batches, {@link SplitReader#fetchNextRecords(Duration)} may be
   * overridden instead
   * @param duration how to potentially block for
   * @return a record or null
   * @throws IOException
   */
  @Nullable
  OUT fetchNextRecord(Duration duration) throws IOException;

  /**
   * Returns a group of records, if no records can be returned within the duration
   * return an empty list
   * @param duration
   * @return
   * @throws IOException
   */
  default List<OUT> fetchNextRecords(Duration duration) throws IOException {
    OUT rec = fetchNextRecord(duration);
    if (rec != null)  {
      return Collections.singletonList(fetchNextRecord(duration));
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * If you want to support extracting timestamps, override this method
   * @param record
   * @return
   */
  @Nullable
  default Long extractTimestamp(OUT record) {
    return null;
  }

  /**
   * TODO: what should this do?
   * ???
   */
  void wakeup();
}
