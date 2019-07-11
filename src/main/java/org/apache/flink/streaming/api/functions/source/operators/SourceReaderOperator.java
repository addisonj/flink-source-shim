package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.functions.source.Source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceOutput;
import org.apache.flink.streaming.api.functions.source.SourceReader;
import org.apache.flink.streaming.api.functions.source.SplitContext;
import org.apache.flink.streaming.api.functions.source.types.ReaderStatus;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SourceReaderOperator<SplitT extends SourceSplit, OUT>
    extends AbstractStreamOperator<OUT>
    implements OneInputStreamOperator<ReadRequest<SplitT>, OUT>,
        ProcessingTimeCallback,
        OutputTypeConfigurable<OUT> {
  private final long WORKER_CHECK_INT = 1000L;
  private final String SPLIT_STATE_NAME = "split_states";

  private final Source<OUT, SplitT, ?> source;
  private final SplitStateAgg<SplitT> aggFunc;
  private final String globalAggName;

  private String readerName;
  private ListState<byte[]> splitStates;
  private GlobalAggregateManager globalAgg;
  private ReaderWorker worker;
  private Thread workerThread;
  private SourceFunction.SourceContext<OUT> srcCtx;
  private Exception workerException;
  private TypeInformation<OUT> outputTypeInfo;
  private ExecutionConfig executionConfig;

  protected SourceFunction.SourceContext<OUT> buildSourceContext(Long idleTimeout) {
    return StreamSourceContexts.getSourceContext(
        getOperatorConfig().getTimeCharacteristic(),
        getProcessingTimeService(),
        getContainingTask().getCheckpointLock(),
        getContainingTask().getStreamStatusMaintainer(),
        output,
        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
        idleTimeout);
  }

  public SourceReaderOperator(Source<OUT, SplitT, ?> source) {
    this.source = source;
    this.globalAggName = SplitStateAgg.stateName(this.source.sourceName());
    this.aggFunc = new SplitStateAgg<>();
  }

  @Override
  public void open() throws Exception {
    super.open();
    srcCtx = buildSourceContext(-1L);
    SourceReader<SplitT, OUT> reader = source.createReader(srcCtx);
    if (reader instanceof OutputTypeConfigurable) {
      ((OutputTypeConfigurable) reader).setOutputType(outputTypeInfo, executionConfig);
    }
    List<SplitT> initialSplits = new ArrayList<>();
    for (byte[] splitBytes : splitStates.get()) {
      initialSplits.add(source.getSplitSerializer().deserialize(1, splitBytes));
    }
    worker = new ReaderWorker(reader, srcCtx, initialSplits);
    workerThread = new Thread(this.worker);
    workerThread.run();
    // every second, check on our worker thread, units are mills
    getProcessingTimeService().scheduleAtFixedRate(this, WORKER_CHECK_INT, WORKER_CHECK_INT);
    // signal to the global state we are ready for splits
    readerName =
        getOperatorName()
            + "-"
            + getRuntimeContext().getIndexOfThisSubtask()
            + "-"
            + getRuntimeContext().getMaxNumberOfParallelSubtasks();
    globalAgg = getRuntimeContext().getGlobalAggregateManager();
    globalAgg.updateGlobalAggregate(
        globalAggName, SourceStateCommand.notifyNewReader(readerName, reader.location()), aggFunc);
  }

  @Override
  public void processElement(StreamRecord<ReadRequest<SplitT>> element) throws Exception {
    this.worker.addSplits(Collections.singletonList(element.getValue().getSplit()));
    checkWorkerFinished();
  }

  private void checkWorkerFinished() throws Exception {
    if (!workerThread.isAlive()) {
      if (worker.exceptionalResult().isPresent()) {
        workerException = worker.exceptionalResult().get();
      }
      close();
    }
  }

  protected void requestMoreSplits() throws IOException {
    globalAgg.updateGlobalAggregate(
        globalAggName, SourceStateCommand.notifyAndRequestMore(readerName), aggFunc);
  }

  @Override
  public void close() throws Exception {
    if (workerException != null) {
      throw workerException;
    }
    super.close();
    srcCtx.close();
  }

  @Override
  public void onProcessingTime(long timestamp) throws Exception {
    checkWorkerFinished();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    splitStates.clear();
    for (SplitT state : this.worker.snapshotState()) {
      splitStates.add(source.getSplitSerializer().serialize(state));
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    splitStates =
        getRuntimeContext()
            .getListState(
                new ListStateDescriptor<>(
                    SPLIT_STATE_NAME, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
  }

  @Override
  public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
    this.outputTypeInfo = outTypeInfo;
    this.executionConfig = executionConfig;
  }

  class SourceOutputImpl implements SourceOutput<OUT> {
    private final SourceFunction.SourceContext<OUT> ctx;

    public SourceOutputImpl(SourceFunction.SourceContext<OUT> ctx) {
      this.ctx = ctx;
    }

    @Override
    public void emitRecord(OUT record) {
      ctx.collect(record);
    }

    @Override
    public void emitRecord(OUT record, long timestamp) {
      ctx.collectWithTimestamp(record, timestamp);
    }

    @Override
    public void emitWatermark(Watermark watermark) {
      ctx.emitWatermark(watermark);
    }

    @Override
    public void markIdle() {
      ctx.markAsTemporarilyIdle();
    }
  }

  class SplitContextImpl implements SplitContext {

    @Override
    public void requestNewSplit() throws IOException {
      requestMoreSplits();
    }
  }

  class ReaderWorker implements Runnable {
    private final SourceFunction.SourceContext<OUT> ctx;
    private final SourceReader<SplitT, OUT> reader;
    private final SourceOutputImpl output;
    private final List<SplitT> initialSplits;

    private boolean running;
    private Exception errorExp;

    public ReaderWorker(
        SourceReader<SplitT, OUT> reader,
        SourceFunction.SourceContext<OUT> ctx,
        List<SplitT> initialSplits) {
      this.reader = reader;
      this.ctx = ctx;
      this.initialSplits = initialSplits;
      this.output = new SourceOutputImpl(this.ctx);
    }

    public void addSplits(List<SplitT> splits) throws IOException {
      synchronized (this.ctx.getCheckpointLock()) {
        this.reader.addSplits(splits);
      }
    }

    public Optional<Exception> exceptionalResult() {
      return Optional.ofNullable(errorExp);
    }

    public void stop() {
      running = false;
    }

    public List<SplitT> snapshotState() {
      return reader.snapshotState();
    }

    @Override
    public void run() {
      // TODO: refactor this to use a mailbox
      running = true;
      try {
        reader.addSplits(initialSplits);
        reader.start(new SplitContextImpl());
      } catch (IOException exp) {
        errorExp = new RuntimeException("failed when starting", exp);
        running = false;
      }

      while (running) {
        CompletableFuture<?> startReadLoop;
        try {
          startReadLoop = reader.available();
        } catch (IOException exp) {
          errorExp = new RuntimeException("failed when checking availiable", exp);
          running = false;
          break;
        }
        CompletableFuture<Boolean> finishRead =
            startReadLoop.thenApply(
                (v) -> {
                  ReaderStatus status = ReaderStatus.MORE_AVAILABLE;
                  while (status == ReaderStatus.MORE_AVAILABLE) {
                    synchronized (ctx.getCheckpointLock()) {
                      try {
                        status = reader.emitNext(output);
                      } catch (IOException exp) {
                        errorExp = new RuntimeException("failed when emitting record", exp);
                        return false;
                      }
                    }
                  }
                  // we should go back to the top of loop and wait for the reader to signal us
                  // again
                  if (status == ReaderStatus.NOTHING_AVAILABLE) {
                    return true;
                  } else {
                    // the reader is indicating it is all done, nothing left to do
                    assert status == ReaderStatus.END_OF_SPLIT_DATA;
                    return false;
                  }
                });
        try {
          running = finishRead.get();
        } catch (InterruptedException | ExecutionException exp) {
          // TODO: probably change this? if we get interrupted, we may want to just swallow it
          errorExp = new RuntimeException("failed to resolve worker loop", exp);
        }
      }
    }
  }
}
