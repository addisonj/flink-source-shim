package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.types.SourceSplit;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;

public abstract class AbstractSyncSourceReader<SplitT extends SourceSplit, OUT>
    implements SourceReader<SplitT, OUT>, OutputTypeConfigurable<OUT> {
  protected final SplitReaderFactory<SplitT, OUT> factory;

  public AbstractSyncSourceReader(SplitReaderFactory<SplitT, OUT> factory) {
    this.factory = factory;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
    if (factory instanceof OutputTypeConfigurable) {
      ((OutputTypeConfigurable) factory).setOutputType(outTypeInfo, executionConfig);
    }
  }
}
