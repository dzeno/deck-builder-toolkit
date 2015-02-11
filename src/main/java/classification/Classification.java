package classification;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.*;

public class Classification {

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSource<String> conditionalInput = env.readTextFile(Config.pathToConditionals());
    DataSource<String> sumInput = env.readTextFile(Config.pathToSums());

    DataSet<Tuple3<String, String, Long>> conditionals = conditionalInput.map(new ConditionalReader());
    DataSet<Tuple2<String, Long>> sums = sumInput.map(new SumReader());

    DataSource<String> testData = env.readTextFile(Config.pathToTestSet());

    DataSet<Tuple3<String, String, Double>> classifiedDataPoints = testData.map(new Classifier())
            .withBroadcastSet(conditionals, "conditionals")
            .withBroadcastSet(sums, "sums");

    //DataSource<Tuple3<String, String, Long>> vocabulary = conditionals.distinct(1).sum(1);
    classifiedDataPoints.writeAsCsv(Config.pathToOutput(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

    env.execute();
  }

    public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Long>> {

    @Override
    public Tuple3<String, String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple3<String, String, Long>(elements[0], elements[1], Long.parseLong(elements[2]));
    }
  }

  public static class SumReader implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String s) throws Exception {
      String[] elements = s.split("\t");
      return new Tuple2<String, Long>(elements[0], Long.parseLong(elements[1]));
    }
  }


  public static class Classifier extends RichMapFunction<String, Tuple3<String, String, Double>>  {

     final private Map<String, Map<String, Long>> wordCounts = Maps.newHashMap();
     final private Map<String, Long> wordSums = Maps.newHashMap();

      HashSet<String> distinctWords = new HashSet<String>();

     @Override
     public void open(Configuration parameters) throws Exception {
         super.open(parameters);

         Collection<Tuple2<String, Long>> sums = getRuntimeContext().getBroadcastVariable("sums");
         for (Tuple2<String, Long> sum : sums)
             wordSums.put(sum.f0, sum.f1);

         Collection<Tuple3<String, String, Long>> conditionals = getRuntimeContext().getBroadcastVariable("conditionals");
         for (Tuple3<String, String, Long> conditional : conditionals){
             if( ! wordCounts.containsKey(conditional.f0)){
                 wordCounts.put(conditional.f0, new HashMap<String , Long>());
             }
             wordCounts.get(conditional.f0).put(conditional.f1, conditional.f2);
             distinctWords.add(conditional.f1);
         }
     }

     @Override
     public Tuple3<String, String, Double> map(String line) throws Exception {

         String[] tokens = line.split("\t");
         String player_name = tokens[0];
         String score = tokens[1];
         String event = tokens[2];
         String arch = tokens[3];
         String[] terms = tokens[4].split("&&");

         double k = Config.getSmoothingParameter();
         int vocabularySize = distinctWords.size();

         double maxProbability = Double.NEGATIVE_INFINITY;
         String predictionLabel = "";
         double prob;

         //for (String term : terms) vocabularySize += Integer.parseInt(term.split("\\$\\$")[0]);

             Long N = 0L;
         for (Long Ntmp : wordSums.values()){
             N += Ntmp;
         }
         for(String key : wordCounts.keySet()){
             //prob = Math.log((double)wordSums.get(key) / (double)N);
             prob = 0;

             String[] card; String name; Long nb;
             for (String term : terms) {
                 card = term.split("\\$\\$");
                 name = card[1];
                 nb = Long.parseLong(card[0]);

                 if (wordCounts.get(key).containsKey(name)) {
                     prob += Math.log((double) nb * (wordCounts.get(key).get(name) + k) /
                             (wordSums.get(key) + (vocabularySize*k)));
                 }
                 else {
                     prob += Math.log( nb * k / (wordSums.get(key) +  (vocabularySize*k)));
                 }
             }
             if(prob > maxProbability){
                 maxProbability = prob;
                 predictionLabel = key;
             }
         }
         return new Tuple3<String, String, Double>(arch, predictionLabel, Math.exp(maxProbability));
     }
  }
}