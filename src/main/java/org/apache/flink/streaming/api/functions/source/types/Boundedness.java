package org.apache.flink.streaming.api.functions.source.types;

/**
 * The boundedness of the source: "bounded" for the currently available data (batch style),
 * "continuous unbounded" for a continuous streaming style source.
 */
public enum Boundedness {

  /**
   * A bounded source processes the data that is currently available and will end after that.
   *
   * <p>When a source produces a bounded stream, the runtime may activate additional optimizations
   * that are suitable only for bounded input. Incorrectly producing unbounded data when the source
   * is set to produce a bounded stream will often result in programs that do not output any results
   * and may eventually fail due to runtime errors (out of memory or storage).
   */
  BOUNDED,

  /**
   * A continuous unbounded source continuously processes all data as it comes.
   *
   * <p>The source may run forever (until the program is terminated) or might actually end at some
   * point, based on some source-specific conditions. Because that is not transparent to the
   * runtime, the runtime will use an execution mode for continuous unbounded streams whenever this
   * mode is chosen.
   */
  CONTINUOUS_UNBOUNDED
}
