package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.ReaderStatus;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The base interface for a source. All these methods are called from a single thread
 *
 * @param <SplitT> the type of the split
 * @param <OUT> the output type
 */
public interface SourceReader<SplitT, OUT> {
  /**
   * Indicates the location for a reader, by default, this is just a random string
   *
   * <p>TODO: I added this call for getting a reader location, that wasn't in the current designs
   *
   * @return
   */
  default ReaderLocation location() {
    return ReaderLocation.randomLocation();
  }
  /**
   * Starts off a reader
   *
   * <p>TODO: I added an argument here with a new class that allows a reader to request more splits
   *
   * @throws IOException
   */
  void start(SplitContext ctx) throws IOException;

  /**
   * Returns a future that indicates when the reader should be checked again
   *
   * @return the future that will resolve when to do something
   * @throws IOException
   */
  CompletableFuture<?> available() throws IOException;

  /**
   * This blocks and reads splits and writes to an operator returns a status indicate what should
   * happen next 1. call again right away 2. call once {@link SourceReader#available} returns 3. no
   * more splits, finished executing
   *
   * @param output the call to output
   * @return the status of what to do next
   * @throws IOException
   */
  ReaderStatus emitNext(SourceOutput<OUT> output) throws IOException;

  /**
   * Called to add more splits to the reader
   *
   * @param splits the splits to add
   * @throws IOException
   */
  void addSplits(List<SplitT> splits) throws IOException;

  /**
   * Should return a representation of splits, SplitT should also have progress (probably?) in the
   * event of restore
   *
   * @return
   */
  List<SplitT> snapshotState();
}
