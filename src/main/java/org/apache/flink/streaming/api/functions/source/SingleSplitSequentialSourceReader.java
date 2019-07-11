package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.ReaderStatus;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Implements a reader that will sequentially iterate through a queue of splits
 *
 * @param <SplitT>
 * @param <OUT>
 */
public class SingleSplitSequentialSourceReader<SplitT extends SourceSplit, OUT>
    extends AbstractSyncSourceReader<SplitT, OUT> {
  private final Queue<SplitT> queue;
  private final Duration pollInterval;

  private SplitContext ctx;
  private SplitReader<OUT> currentReader;
  private SplitT currentSplit;
  private boolean enumeratorFinished;
  private CompletableFuture<Boolean> newSplitFuture;

  public SingleSplitSequentialSourceReader(
      SplitReaderFactory<SplitT, OUT> factory, Duration pollInterval) {
    super(factory);
    this.queue = new LinkedList<>();
    this.pollInterval = pollInterval;
    this.enumeratorFinished = false;
  }

  public SingleSplitSequentialSourceReader(SplitReaderFactory<SplitT, OUT> factory) {
    this(factory, Duration.ofSeconds(10));
  }

  @Override
  public void start(SplitContext ctx) throws IOException {
    this.ctx = ctx;
  }

  @Override
  public CompletableFuture<?> available() throws IOException {
    if (newSplitFuture == null) {
      newSplitFuture = new CompletableFuture<>();
    }
    return newSplitFuture;
  }

  @Override
  public List<SplitT> snapshotState() {
    ArrayList<SplitT> state = new ArrayList<>();
    state.add(currentSplit);
    state.addAll(queue);
    return state;
  }

  @Override
  public ReaderStatus emitNext(SourceOutput<OUT> output) throws IOException {
    if (currentReader == null || currentSplit == null) {
      if (queue.isEmpty() && enumeratorFinished) {
        return ReaderStatus.END_OF_SPLIT_DATA;
      } else if (queue.isEmpty()) {
        return ReaderStatus.NOTHING_AVAILABLE;
      } else {
        currentSplit = queue.poll();
        currentReader = factory.create(currentSplit);
      }
    }

    if (currentSplit.isFinished()) {
      currentSplit = null;
      currentReader = null;
      // we fall through and send more available so we can try and get the next group
      // if there is nothing left, we will immediately send `NOTHING_AVAILIABLE` on next loop
    } else {
      List<OUT> recs = currentReader.fetchNextRecords(pollInterval);
      recs.forEach(
          (rec) -> {
            Long ts = currentReader.extractTimestamp(rec);
            if (ts != null) {
              output.emitRecord(rec, ts);
            } else {
              output.emitRecord(rec);
            }
          });
    }
    return ReaderStatus.MORE_AVAILABLE;
  }

  @Override
  public void addSplits(List<SplitT> splits) throws IOException {
    queue.addAll(splits);
    // notify any waiting that we may have more stuff to do
    if (newSplitFuture != null) {
      newSplitFuture.complete(true);
      newSplitFuture = null;
    }
  }

  @Override
  public void enumeratorFinished() {
    enumeratorFinished = true;
  }
}
