package org.apache.flink.streaming.api.functions.source.operators;

import org.apache.flink.streaming.api.functions.source.types.ReaderLocation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;

import java.util.Optional;

public class SourceStateCommand<SplitT extends SourceSplit> {
  private SourceStateCommands type;
  private String readerName;
  private ReaderLocation location;
  private ReaderCommandState readerState;

  private SourceStateCommand(SourceStateCommands type) {
    this.type = type;
  }

  public SourceStateCommands getType() {
    return type;
  }

  public Optional<String> getReaderName() {
    return Optional.ofNullable(readerName);
  }

  public Optional<ReaderLocation> getReaderLocation() {
    return Optional.ofNullable(location);
  }

  public Optional<ReaderCommandState> getReaderState() {
    return Optional.ofNullable(readerState);
  }

  public static <SplitT extends SourceSplit> SourceStateCommand<SplitT> querySplit() {
    return new SourceStateCommand<>(SourceStateCommands.GET_STATE);
  }

  public static <SplitT extends SourceSplit> SourceStateCommand<SplitT> notifyNewReader(
      String readerName, ReaderLocation location) {
    SourceStateCommand<SplitT> newReaderCommand =
        new SourceStateCommand<>(SourceStateCommands.UPDATE_READER_STATE);
    newReaderCommand.readerName = readerName;
    newReaderCommand.location = location;
    newReaderCommand.readerState = ReaderCommandState.NEW_READER;
    return newReaderCommand;
  }

  public static <SplitT extends SourceSplit> SourceStateCommand<SplitT> notifyAndRequestMore(
      String readerName) {
    return readerUpdateCommand(readerName, ReaderCommandState.SEND_MORE);
  }

  public static <SplitT extends SourceSplit> SourceStateCommand<SplitT> notifyAndFinished(
      String readerName) {
    return readerUpdateCommand(readerName, ReaderCommandState.FINISHED);
  }

  private static <SplitT extends SourceSplit> SourceStateCommand<SplitT> readerUpdateCommand(
      String readerName, ReaderCommandState state) {
    SourceStateCommand<SplitT> updateCommand =
        new SourceStateCommand<>(SourceStateCommands.UPDATE_READER_STATE);
    updateCommand.readerName = readerName;
    updateCommand.readerState = state;
    return updateCommand;
  }

  public enum ReaderCommandState {
    NEW_READER, // indicates that the reader is waiting for initial splits
    SEND_MORE, // the reader is ready for more splits
    FINISHED // the reader is finished with all splits and will not accept any new splits
  }

  /** this is how we indicate what type of events we are doing for state updates */
  public enum SourceStateCommands {
    GET_STATE,
    UPDATE_READER_STATE
  }
}
