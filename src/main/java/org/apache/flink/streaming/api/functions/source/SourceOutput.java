package org.apache.flink.streaming.api.functions.source;

public interface SourceOutput<OUT> extends WatermarkOutput {
  void emitRecord(OUT record);

  void emitRecord(OUT record, long timestamp);
}
