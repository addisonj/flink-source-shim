package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.util.Optional;

public class SplitStateAgg<SplitT extends SourceSplit>
    implements AggregateFunction<
        SourceStateCommand<SplitT>, SplitQuery<SplitT>, SplitQuery<SplitT>> {

  public static String stateName(String prefix) {
    return prefix + "_global_state";
  }

  @Override
  public SplitQuery<SplitT> createAccumulator() {
    return new SplitQuery<>();
  }

  @Override
  public SplitQuery<SplitT> add(SourceStateCommand<SplitT> value, SplitQuery<SplitT> accumulator) {
    if (value.getType() == SourceStateCommand.SourceStateCommands.UPDATE_READER_STATE) {
      Optional<String> readerName = value.getReaderName();
      if (!readerName.isPresent()) {
        throw new RuntimeException("command was update but did not include reader name");
      }
      Optional<ReaderState<SplitT>> existingState = accumulator.getReaderState(readerName.get());
      ReaderState<SplitT> updatedState;
      if (existingState.isPresent()) {
        if (value.getReaderState().isPresent()) {
          updatedState = existingState.get().updateState(value.getReaderState().get());
        } else {
          throw new RuntimeException("reader did not provide a state or split updates");
        }
      } else {
        assert value.getReaderState().isPresent()
            && value.getReaderState().get() == SourceStateCommand.ReaderCommandState.NEW_READER;
        if (value.getReaderName().isPresent() && value.getReaderLocation().isPresent()) {
          updatedState =
              new ReaderState<>(value.getReaderName().get(), value.getReaderLocation().get());
        } else {
          throw new RuntimeException("reader did not provide name or location");
        }
      }
      return accumulator.updateReader(readerName.get(), updatedState);
    } else {
      return accumulator;
    }
  }

  @Override
  public SplitQuery<SplitT> getResult(SplitQuery<SplitT> accumulator) {
    return accumulator;
  }

  @Override
  public SplitQuery<SplitT> merge(SplitQuery<SplitT> a, SplitQuery<SplitT> b) {
    return a.merge(b);
  }
}
