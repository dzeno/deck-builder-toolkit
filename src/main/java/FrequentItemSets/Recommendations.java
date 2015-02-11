package FrequentItemSets;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author Dzenan Softic
 * @author Tanguy Racinet
 */
public class Recommendations {

    public static void main(String[] args) throws Exception {


        // Recommendations: Tuple3<card, score(support*ratio), usersCards>
        DataSet<Tuple3<String, Double, Integer>> Recommendations = frequentSets
                // generates item sets for that user
                .map(new RecommendationSystem()).withBroadcastSet(UserCards, "UserCards")
                        // removes sets not frequent enough (Config.getScoreThreshold)
                .filter(new FilterNonpresent())
                        // print out the top X sets with 3, 2, 1 and 0 cards from the user
                .groupBy(2).sortGroup(2, Order.DESCENDING).first(3);

        Recommendations.writeAsCsv(Config.pathToRecommendations(), "\n", "\t", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }


    public static class RecommendationSystem extends RichMapFunction<Tuple3<String, Double, Double>, Tuple3<String, Double, Integer>> {

        final private List<String> cards = new ArrayList<String>();

        /**
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Collection<Tuple2<String, Integer>> userCards = getRuntimeContext().getBroadcastVariable("UserCards");
            for (Tuple2<String, Integer> card : userCards)
                cards.add(card.f0);
        }

        /**
         * @param line
         * @return
         * @throws Exception
         */
        @Override
        public Tuple3<String, Double, Integer> map(Tuple3<String, Double, Double> line) throws Exception {
            String[] tokens = line.f0.split("&&");
            Integer number = 0;

            for(String card : cards){
                number += Arrays.asList(tokens).contains(card)? 1 : 0;
            }
            return new Tuple3<String, Double, Integer>(line.f0, line.f1 * line.f2, number);
        }
    }
}
