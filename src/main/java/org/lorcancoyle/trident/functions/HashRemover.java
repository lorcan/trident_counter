package org.lorcancoyle.trident.functions;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class HashRemover extends BaseFunction {

  public void execute(TridentTuple tuple, TridentCollector collector) {
    String word = tuple.getString(0);
    String output = word.substring(1);
    collector.emit(new Values(output));
  }
}
