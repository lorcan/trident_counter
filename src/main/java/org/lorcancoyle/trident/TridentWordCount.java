package org.lorcancoyle.trident;

import org.lorcancoyle.trident.functions.HashRemover;
import org.lorcancoyle.trident.functions.Split;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TridentWordCount {

  public static StormTopology buildTopology(LocalDRPC drpc) {
    FixedBatchSpout spout = new FixedBatchSpout(
        new Fields("sentence"),
        3,
        new Values("#jools #2012pics #happynewyear #nye2011 #newyear #happynewyear"),
        new Values(
            "#kilfinanetweetup #rtetimeforchange #nyedublin #kilfinanetweetup #ge16 #happynewyear #iowa #ia #eu #eu2012dk #tourism"),
        new Values(
            "#cork #titanic #cobh #fightracism #celebraterights #fightracism #happynewyear #fightracism #celebraterights #clonea"),
        new Values(
            "#christmas #householdtax #fb #fightausterity #mfc12 #marian #payyourtaxesathome #cork #eastenders #rte50 #eastenders "),
        new Values(
            "#sadstart22012 #rte1 #wasters #mammysarewonderful #eastenders #rte #faircity #oh #jedward #olympia #panto #darts #dail #lookingwest"));
    spout.setCycle(true);

    TridentTopology topology = new TridentTopology();
    topology.newStream("spout1", spout)
    //
        .parallelismHint(16).each(new Fields("sentence"), new Split(), new Fields("word"))
        //
        .each(new Fields("word"), new HashRemover(), new Fields("cleanWord"))
        //
        .groupBy(new Fields("cleanWord"))
        //
        // .aggregate(new Fields("cleanWord"), new Count(), new
        // Fields("count"))//
        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))//
        .newValuesStream().each(new Fields("cleanWord", "count"), new Debug())
        //

        .parallelismHint(16);

    /* topology.newDRPCStream("hashtagQuery", drpc).each(new Fields("args"), new
     * Split(), new Fields("word")) .groupBy(new
     * Fields("word")).stateQuery(wordCounts, new Fields("word"), new MapGet(),
     * new Fields("count")) .each(new Fields("count"), new
     * FilterNull()).aggregate(new Fields("count"), new Sum(), new
     * Fields("sum")); */return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setDebug(false);
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 100; i++) {
        System.out.println("DRPC RESULT jools: " + drpc.execute("hashtagQuery", " jools"));
        System.out.println("DRPC RESULT 2012pics: " + drpc.execute("hashtagQuery", " 2012pics"));
        System.out.println("DRPC RESULT happynewyear: " + drpc.execute("hashtagQuery", " happynewyear"));

        Thread.sleep(1000);
      }
    } else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
    }
  }
}
