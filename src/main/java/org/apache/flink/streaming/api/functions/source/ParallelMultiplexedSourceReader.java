package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.functions.source.types.ReaderStatus;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ParallelMultiplexedSourceReader<SplitT extends SourceSplit, OUT>
    extends AbstractAsyncSourceReader<SplitT, OUT> {
  private final Map<String, ReaderSplit> readers;

  private SplitContext ctx;
  private boolean enumeratorFinished;
  private CompletableFuture<Boolean> waitingFuture;

  public ParallelMultiplexedSourceReader(AsyncSplitReaderFactory<SplitT, OUT> factory) {
    super(factory);
    this.readers = new HashMap<>();
    this.enumeratorFinished = false;
  }

  @Override
  public void start(SplitContext ctx) throws IOException {
    this.ctx = ctx;
  }

  @Override
  public CompletableFuture<?> available() throws IOException {
    if (waitingFuture == null) {
      waitingFuture = new CompletableFuture<>();
    }
    return waitingFuture;
  }

  @Override
  public List<SplitT> snapshotState() {
    return readers.values().stream().map(ReaderSplit::getSplit).collect(Collectors.toList());
  }

  @Override
  public ReaderStatus emitNext(SourceOutput<OUT> output) throws IOException {
    if (readers.isEmpty()) {
      return enumeratorFinished ? ReaderStatus.END_OF_SPLIT_DATA : ReaderStatus.NOTHING_AVAILABLE;
    }

    ArrayList<String> toRemove = new ArrayList<>();
    ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<String, ReaderSplit> entry : readers.entrySet()) {
      ReaderSplit rs = entry.getValue();
      AsyncSplitReader<OUT> reader = rs.getReader();
      if (rs.isFinished()) {
        toRemove.add(entry.getKey());
      } else {
        CompletableFuture<Void> res =
            reader
                .fetchNextRecords()
                .thenAccept(
                    (recs) ->
                        // TODO somewhere here, we would change this to do per-partition watermarks
                        // and event time alignment
                        recs.forEach(
                            (rec) -> {
                              Long ts = reader.extractTimestamp(rec);
                              if (ts != null) {
                                output.emitRecord(rec, ts);
                              } else {
                                output.emitRecord(rec);
                              }
                            }));
        futures.add(res);
      }
    }
    toRemove.forEach(readers::remove);
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])).get();
      return ReaderStatus.MORE_AVAILABLE;
    } catch (InterruptedException | ExecutionException exp) {
      throw new IOException("failure when resolving futures", exp);
    }
  }

  @Override
  public void addSplits(List<SplitT> splits) throws IOException {
    splits.forEach(this::addReaderSplit);
    if (waitingFuture != null) {
      waitingFuture.complete(true);
      waitingFuture = null;
    }
  }

  @Override
  public void enumeratorFinished() {
    enumeratorFinished = true;
  }

  private void addReaderSplit(SplitT split) {
    AsyncSplitReader<OUT> reader = factory.create(split);
    readers.put(split.splitId(), new ReaderSplit(reader, split));
  }

  private class ReaderSplit {
    private final AsyncSplitReader<OUT> reader;
    private final SplitT split;

    public ReaderSplit(AsyncSplitReader<OUT> reader, SplitT split) {
      this.reader = reader;
      this.split = split;
    }

    public AsyncSplitReader<OUT> getReader() {
      return reader;
    }

    public SplitT getSplit() {
      return split;
    }

    public boolean isFinished() {
      return split.isFinished();
    }
  }
}
