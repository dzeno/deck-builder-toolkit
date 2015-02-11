package FrequentItemSets;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author Dzenan Softic
 * @author Tanguy Racinet
 */
public class Apriori {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> input = env.readTextFile(Config.pathToTrainingSet());

        // read input in tuple5<Archetype, PlayerName, score, card, numberOfOccurrences>
        DataSet<Tuple5<String, String, Double, String, Double>> labeledTerms = input.flatMap(new DataReader());

        // Whole vocabulary : distinct cards present in working Archetype
        DataSet<Tuple1<String>> vocabulary = labeledTerms.distinct(3).project(3).types(String.class);

        // tuple3<CardsSet, Support, VictoryRatio>
        IterativeDataSet<Tuple3<String, Double, Double>> candidates = labeledTerms.distinct(3).project(3, 4, 2).types(String.class, Double.class, Double.class).iterate(3);

        env.execute();
    }


    public static class GenerateCandidates implements CrossFunction<Tuple3<String, Double, Double>, Tuple1<String>, Tuple3<String, Double, Double>> {
        @Override
        public Tuple3<String, Double, Double> cross(Tuple3<String, Double, Double> line, Tuple1<String> voc) throws Exception {

            String tokens[] = (line.f0+"&&"+voc.f0).split("&&");
            Arrays.sort(tokens);
            String result = StringUtils.join(tokens, "&&");
            return new Tuple3<String, Double, Double>(result, 0.0, line.f2);
        }
    }

    public static class CandidatesToDeck extends RichMapFunction<Tuple3<String, Double, Double>, Tuple3<String, Double, Double>> {

        final private HashMap<Tuple3<String, String, Double>, List<String>> deckLists = Maps.newHashMap();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Collection<Tuple5<String, String, Double, String, Long>> decks = getRuntimeContext().getBroadcastVariable("decks");

            for (Tuple5<String, String, Double, String, Long> deck : decks){

                Tuple3<String, String, Double> key = new Tuple3<String, String, Double>(deck.f0, deck.f1, deck.f2);
                if( ! deckLists.containsKey(key) ){
                    deckLists.put(new Tuple3<String, String, Double>(deck.f0, deck.f1, deck.f2), new ArrayList<String>());
                }
                deckLists.get(key).add(deck.f3);
            }
        }
    }

    private static class SynergyFilter implements FilterFunction<Tuple3<String, Double, Double>> {
        @Override
        public boolean filter(Tuple3<String, Double, Double> count) throws Exception {
            return count.f1 >= Config.getSupportThreshold();
        }
    }

}
