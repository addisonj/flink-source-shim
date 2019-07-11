package org.apache.flink.streaming.api.functions.source.file;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.source.SingleSplitSequentialSourceReader;
import org.apache.flink.streaming.api.functions.source.Source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceReader;
import org.apache.flink.streaming.api.functions.source.SplitEnumerator;
import org.apache.flink.streaming.api.functions.source.SplitReader;
import org.apache.flink.streaming.api.functions.source.SplitReaderFactory;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.functions.source.types.Boundedness;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;

class FileSplit implements SourceSplit {
  private final TimestampedFileInputSplit split;
  private boolean finished;

  public FileSplit(TimestampedFileInputSplit split) {
    this.split = split;
    finished = false;
  }

  @Override
  public String splitId() {
    return split.getPath().toString();
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  public void fileFinished() {
    finished = true;
  }

  public TimestampedFileInputSplit getInputSplit() {
    return split;
  }
}

class FileEnumeratorState {}

public class FileSource<OUT> implements Source<OUT, FileSplit, FileEnumeratorState> {
  private final FileInputFormat<OUT> inputFormat;
  private final String startDir;

  public FileSource(String directory, FileInputFormat<OUT> fileFormat) {
    this.startDir = directory;
    this.inputFormat = fileFormat;
  }

  @Override
  public String sourceName() {
    return "file_reader_" + startDir;
  }

  @Override
  public boolean supportsBoundedness(Boundedness boundedness) {
    // support both
    return true;
  }

  @Override
  public SourceReader<FileSplit, OUT> createReader(SourceFunction.SourceContext<OUT> ctx)
      throws IOException {
    return new SingleSplitSequentialSourceReader<>(new FileReaderFactory());
  }

  @Override
  public SplitEnumerator<FileSplit, FileEnumeratorState> createEnumerator(Boundedness mode)
      throws IOException {
    return null;
  }

  @Override
  public SplitEnumerator<FileSplit, FileEnumeratorState> restoreEnumerator(
      Boundedness mode, FileEnumeratorState checkpoint) throws IOException {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<FileSplit> getSplitSerializer() {
    return null;
  }

  @Override
  public SimpleVersionedSerializer<FileEnumeratorState> getEnumeratorCheckpointSerializer() {
    return null;
  }

  public class FileReader implements SplitReader<OUT> {
    private final FileSplit split;
    private final TimestampedFileInputSplit fileSplit;
    private final FileInputFormat<OUT> format;

    private boolean opened = false;
    private OUT nextEl;

    public FileReader(
        TypeSerializer<OUT> serializer, FileInputFormat<OUT> format, FileSplit split) {
      this.nextEl = serializer.createInstance();
      this.format = format;
      this.split = split;
      this.fileSplit = this.split.getInputSplit();
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public OUT fetchNextRecord(Duration duration) throws IOException {
      if (!opened) {
        if (format instanceof CheckpointableInputFormat && fileSplit.getSplitState() != null) {
          ((CheckpointableInputFormat) format).reopen(fileSplit, fileSplit.getSplitState());
        } else {
          format.open(fileSplit);
        }
        opened = true;
      }
      if (!format.reachedEnd()) {
        nextEl = format.nextRecord(nextEl);
        // TODO: currently, we update state on each record... that isn't quite as efficient
        // perhaps we should expose something higher up?
        if (format instanceof CheckpointableInputFormat) {
          Serializable formatState =
              ((CheckpointableInputFormat<TimestampedFileInputSplit, Serializable>) format)
                  .getCurrentState();
          fileSplit.setSplitState(formatState);
        }
        return nextEl;
      } else {
        split.fileFinished();
        return null;
      }
    }

    @Override
    public void wakeup() {}

    @Override
    public void close() throws IOException {
      format.close();
    }
  }

  public class FileReaderFactory
      implements SplitReaderFactory<FileSplit, OUT>, OutputTypeConfigurable<OUT> {
    private TypeSerializer<OUT> serializer;

    @Override
    public SplitReader<OUT> create(FileSplit split) {
      return new FileReader(serializer, inputFormat, split);
    }

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
      this.serializer = outTypeInfo.createSerializer(executionConfig);
    }
  }
}
