package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.Source;
import org.apache.flink.streaming.api.functions.source.SplitEnumerator;
import org.apache.flink.streaming.api.functions.source.types.Boundedness;
import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Not *really* an operator yet, this is a shim method until you add support into dataStream */
public class SourceOperator<SplitT extends SourceSplit, OUT, EnumCheckT> {

  private class EnumeratorSourceFunc extends RichSourceFunction<ReadRequest<SplitT>>
      implements CheckpointedFunction, CheckpointListener {
    private final Source<OUT, SplitT, EnumCheckT> source;
    private final SplitStateAgg<SplitT> aggFunc;
    private final String globalAggName;
    private final SimpleVersionedSerializer<SplitT> splitSerializer;
    private final SimpleVersionedSerializer<EnumCheckT> enumSerializer;
    private final List<SplitT> pendingSplits;
    private final Map<Long, List<SplitT>> pendingForCheckpoint;
    private final Boundedness bound;

    private SplitEnumerator<SplitT, EnumCheckT> enumerator;

    private ListState<Tuple2<Long, byte[]>> pendingSplitState;
    private ListState<byte[]> enumState;
    private GlobalAggregateManager globalAgg;
    private volatile boolean isRunning;

    EnumeratorSourceFunc(Source<OUT, SplitT, EnumCheckT> source, Boundedness bound) {
      this.aggFunc = new SplitStateAgg<>();
      this.source = source;
      globalAggName = SplitStateAgg.stateName(this.source.sourceName());
      this.bound = bound;
      try {
        this.enumerator = this.source.createEnumerator(bound);
      } catch (IOException exp) {
        throw new RuntimeException("failed to create enumerator", exp);
      }
      this.enumSerializer = this.source.getEnumeratorCheckpointSerializer();
      this.splitSerializer = this.source.getSplitSerializer();
      this.pendingSplits = new ArrayList<>();
      this.pendingForCheckpoint = new HashMap<>();
      isRunning = false;
    }

    @Override
    public void run(SourceContext<ReadRequest<SplitT>> ctx) throws Exception {
      isRunning = true;
      while (isRunning) {
        while (!enumerator.isEndOfInput()) {
          if (!isRunning) {
            break;
          }

          // TODO this is a bit naive perhaps... but it works for now
          globalAgg
              .updateGlobalAggregate(globalAggName, SourceStateCommand.querySplit(), aggFunc)
              .getWaitingReaders()
              .forEach(
                  (reader) -> {
                    ReaderLocation loc = reader.getLocation();
                    synchronized (ctx.getCheckpointLock()) {
                      Optional<SplitT> nextSplit = enumerator.nextSplit(loc);
                      nextSplit.ifPresent(pendingSplits::add);
                      nextSplit
                          .map((split) -> new ReadRequest<>(split, loc))
                          .ifPresent(ctx::collect);
                    }
                  });

          Thread.sleep(1000);
        }

        // done with splits, just wait until all splits are finished
        SplitQuery newState =
            globalAgg.updateGlobalAggregate(
                globalAggName, SourceStateCommand.querySplit(), aggFunc);
        if (newState.getUnfinishedSplits().isEmpty()) {
          break;
        }

        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      globalAgg = ((StreamingRuntimeContext) getRuntimeContext()).getGlobalAggregateManager();
      super.open(parameters);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
      Long checkId = functionSnapshotContext.getCheckpointId();
      // serialize the split state, for all the splits, save them by current checkpoint
      pendingForCheckpoint.put(checkId, pendingSplits);
      // we clear our temp ones we got since last checkpoint
      pendingSplits.clear();
      pendingSplitState.clear();
      // write them to state with checkpoint ids
      pendingForCheckpoint.forEach(
          (cid, splitsForCheck) -> {
            splitsForCheck.forEach(
                (pendingSplit) -> {
                  try {
                    pendingSplitState.add(
                        new Tuple2<>(cid, splitSerializer.serialize(pendingSplit)));
                  } catch (Exception exp) {
                    throw new RuntimeException("failed to serialize split", exp);
                  }
                });
          });

      // serialize enumerator state
      enumState.clear();
      try {
        enumState.add(enumSerializer.serialize(enumerator.snapshotState()));
      } catch (IOException exp) {
        throw new RuntimeException("failed to serialize enumerator state", exp);
      }

      // TODO: still need to capture the global aggregator state
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext)
        throws Exception {
      pendingSplitState =
          functionInitializationContext
              .getOperatorStateStore()
              .getUnionListState(
                  new ListStateDescriptor<>(
                      "pending_splits",
                      TypeInformation.of(new TypeHint<Tuple2<Long, byte[]>>() {})));
      enumState =
          functionInitializationContext
              .getOperatorStateStore()
              .getUnionListState(
                  new ListStateDescriptor<>(
                      "enum_state", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO));

      if (functionInitializationContext.isRestored()) {
        // restore the enumerator state
        // this should only ever have a single element, but we use this just in case
        enumState
            .get()
            .forEach(
                (enumBytes) -> {
                  try {
                    enumerator =
                        source.restoreEnumerator(bound, enumSerializer.deserialize(1, enumBytes));
                  } catch (IOException exp) {
                    throw new RuntimeException(
                        "failed to deserialize enumerator checkpoint state on restore", exp);
                  }
                });

        // TODO: this needs to be somewhat different... I believe we want to
        // not restore anything here and instead use the enumerator.addSplitsBack call
        // however, it may not be all of them (based on checkpoints) and I need to think about it
        // for now, we just restore the state into the map and assume that the enumerator would
        // regen any splits
        // and the next checkpoint will clear out any stale values
        pendingSplitState
            .get()
            .forEach(
                (t) -> {
                  Long checkId = t.f0;
                  try {
                    SplitT split = splitSerializer.deserialize(1, t.f1);
                    pendingForCheckpoint.compute(
                        checkId,
                        (k, v) -> {
                          if (v == null) {
                            return Collections.singletonList(split);
                          } else {
                            v.add(split);
                            return v;
                          }
                        });
                  } catch (IOException exp) {
                    throw new RuntimeException("failed to deserialize split state on restore", exp);
                  }
                });
      }

      // TODO: still need to handle the global state
    }

    /**
     * This function can be called with 3 different cases: 1. Only one set of splits in a single
     * checkpoint outstanding, which should be the most common 2. Multiple sets of splits where the
     * the latest checkpoint is greater than all of them 3. A checkpoint that is has some splits
     * lower than it, and some greater
     *
     * <p>All of these cases can be handled by iterating through and removing all elements less than
     * or equal to the current checkpoint id
     *
     * @param checkpointId
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
      pendingForCheckpoint.keySet().stream()
          .filter((cid) -> cid <= checkpointId)
          .forEach(pendingForCheckpoint::remove);
    }
  }

  private final Source<OUT, SplitT, EnumCheckT> source;

  public SourceOperator(Source<OUT, SplitT, EnumCheckT> source) {
    this.source = source;
  }

  private DataStream<ReadRequest<SplitT>> buildSource(
      Boundedness bound, StreamExecutionEnvironment env) {
    return env.addSource(new EnumeratorSourceFunc(source, bound)).keyBy(ReadRequest::getLocation);
  }

  private SourceReaderOperator<SplitT, OUT> buildTransform() {
    return new SourceReaderOperator<>(source);
  }

  public DataStream<OUT> continuousSource(StreamExecutionEnvironment env) {
    if (!source.supportsBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)) {
      throw new IllegalArgumentException("source does not support continuous operation");
    }

    SourceReaderOperator<SplitT, OUT> op = buildTransform();
    TypeInformation<OUT> outType =
        TypeExtractor.createTypeInfo(op, SourceReaderOperator.class, op.getClass(), 1);
    return buildSource(Boundedness.CONTINUOUS_UNBOUNDED, env)
        .transform("sourceReader", outType, op);
  }

  public DataStream<OUT> boundedSource(StreamExecutionEnvironment env) {
    if (!source.supportsBoundedness(Boundedness.BOUNDED)) {
      throw new IllegalArgumentException("source does not support bounded operation");
    }

    SourceReaderOperator<SplitT, OUT> op = buildTransform();
    TypeInformation<OUT> outType =
        TypeExtractor.createTypeInfo(op, SourceReaderOperator.class, op.getClass(), 1);
    return buildSource(Boundedness.BOUNDED, env).transform("sourceReader", outType, op);
  }
}
