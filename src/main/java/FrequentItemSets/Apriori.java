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

        // read input in tuple5<Archetype, PlayerName, victoryRatio, card, numberOfOccurrences>
        DataSet<Tuple5<String, String, Double, String, Double>> labeledTerms = input.flatMap(new DataReader());

        // Working vocabulary: distinct cards present in working Archetype
        DataSet<Tuple1<String>> vocabulary = labeledTerms.distinct(3).project(3).types(String.class);

        // Candidates: tuple3<CardsSet, Support, VictoryRatio>
        IterativeDataSet<Tuple3<String, Double, Double>> candidates = labeledTerms.distinct(3).project(3, 4, 2).types(String.class, Double.class, Double.class).iterate(3);

        // GeneratedCandidates: tuple3<CardsSet, Support, VictoryRatio>
		DataSet<Tuple3<String, Double, Double>> generatedCandidates = candidates
                // generating candidates : all combinations of the previous generation with the vocabulary
                .cross(vocabulary).with(new GenerateCandidates())
                // removing all duplicated item sets
                .distinct(0)
                // removing sets with duplicated values inside
                .filter(new RemoveDuplicates())
                // generating support and victoryRatio of every candidates for every DeckList
                .map(new CalculateFrequentSets()).withBroadcastSet(labeledTerms, "decks")
                // removing sets not frequent enough ie not interesting (config.getSupportThreshold)
                .filter(new SynergyFilter());

        DataSet<Tuple3<String, Double, Double>> count = candidates.closeWith(generatedCandidates);

        count.writeAsCsv(Config.pathToFrequentSets(), "\n", "\t", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);	
			
        env.execute();
    }

	public static class DataReader implements FlatMapFunction<String, Tuple5<String, String, Double, String, Double>> {
        /**
         * @param line
         * @param collector: Tuple5<Archetype, PlayerName, victoryRatio, card, numberOfOccurrences>
         */
		@Override
        public void flatMap(String line, Collector<Tuple5<String, String, Double, String, Double>> collector) throws Exception {
            try {
                String[] tokens = line.split("\t");
                String player_name = tokens[0];
                String[] eventResult = tokens[1].split(",");
                String event = tokens[2];
                String arch = tokens[3];
                String[] terms = tokens[4].split("&&");

				// calculating thevictoryRatio. Return 0.5 if no results
                Double victories = Double.parseDouble(eventResult[0]);
                Double defeats = Double.parseDouble(eventResult[1]);
                Double victoryRatio = (victories+defeats != 0)? (victories / (victories + defeats)) : 0.5;

                //Broadcasting only the decks lists labeled with the archetype of choice
                if (arch.equals(Config.getArchetype())) {
                    String[] card;
                    String name;
                    Double nb;
                    for (String term : terms) {
                        card = term.split("\\$\\$");
                        name = card[1];
                        nb = Double.parseDouble(card[0]);
                        collector.collect(new Tuple5<String, String, Double, String, Double>(arch, player_name + event, victoryRatio, name, nb));
                    }
                }
            }
            catch(Exception e){
                System.out.println("warning, format problem: "+line);
            }
        }
    }

    public static class GenerateCandidates implements CrossFunction<Tuple3<String, Double, Double>, Tuple1<String>, Tuple3<String, Double, Double>> {
	    /**
         * GenerateCandidates will generate all combinations of previous round candidates with vocabulary
         *
         * @param line: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         * @return result: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         */
		@Override
        public Tuple3<String, Double, Double> cross(Tuple3<String, Double, Double> line, Tuple1<String> voc) throws Exception {

            String tokens[] = (line.f0+"&&"+voc.f0).split("&&");
            Arrays.sort(tokens);
            String result = StringUtils.join(tokens, "&&");
            return new Tuple3<String, Double, Double>(result, 0.0, line.f2);
        }
    }

    public static class CalculateFrequentSets extends RichMapFunction<Tuple3<String, Double, Double>, Tuple3<String, Double, Double>> {

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
		
		/**
         * CalculateFrequentSets will generate the support and victoryRatio of every candidate ItemSet for every DeckList
         *
         * @param line: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         * @return result: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         */
        @Override
        public Tuple3<String, Double, Double> map(Tuple3<String, Double, Double> line) throws Exception {
            String tokens[] = line.f0.split("&&");
            Double support = 0.0;
            Double victoryRatio = 0.0;

            int iteration = getIterationRuntimeContext().getSuperstepNumber();

            for(Tuple3<String, String, Double> key : deckLists.keySet()){
                boolean contains = true;
                for(String token : tokens){
                    contains = contains && deckLists.get(key).contains(token);
                }
				// last iteration: calculate victoryRatio of cardSet
                if (iteration == 3 && contains) {
                        support ++;
                        victoryRatio += key.f2;
                }
                else{
                    support += contains ? 1 : 0;
                }
            }
            return new Tuple3<String, Double, Double>(line.f0, support/deckLists.size(), victoryRatio/support);
        }
    }

    private static class SynergyFilter implements FilterFunction<Tuple3<String, Double, Double>> {
        /**
         * @param line: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         * @return boolean: support > threshold
         */
		@Override
        public boolean filter(Tuple3<String, Double, Double> count) throws Exception {
            return count.f1 >= Config.getSupportThreshold();
        }
    }
	
	private static class RemoveDuplicates implements FilterFunction<Tuple3<String, Double, Double>> {
        /**
         * @param line: Tuple3<frequentItemSet:String, ItemSetSupport:Double, ItemSetVictoryRatio:Double>
         * @return boolean: contains no duplicated cards inside a set
         */
		@Override
        public boolean filter(Tuple3<String, Double, Double> line) throws Exception {
            String[] tokens = line.f0.split("&&");
            return tokens.length == (new HashSet<String>(Arrays.asList(tokens)).size());
        }
    }
}
