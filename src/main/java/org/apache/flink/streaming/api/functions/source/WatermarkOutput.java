package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.streaming.api.watermark.Watermark;

/** An output for watermarks. The output accepts watermarks and idleness (inactivity) status. */
public interface WatermarkOutput {

  /**
   * Emits the given watermark.
   *
   * <p>Emitting a watermark also ends previously marked idleness.
   */
  void emitWatermark(Watermark watermark);

  /**
   * Marks this output as idle, meaning that downstream operations do not wait for watermarks from
   * this output.
   *
   * <p>An output becomes active again as soon as the next watermark is emitted.
   */
  void markIdle();
}
