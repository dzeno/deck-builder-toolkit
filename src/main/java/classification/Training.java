package classification;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class Training {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> input = env.readTextFile(Config.pathToTrainingSet());

        // read input
        DataSet<Tuple3<String, String, Long>> labeledTerms = input.flatMap(new DataReader());

        // conditional counter per word per label
        DataSet<Tuple3<String, String, Long>> termCounts = labeledTerms.groupBy(0, 1).sum(2);

        termCounts.writeAsCsv(Config.pathToConditionals(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        // word counts per label
        DataSet<Tuple2<String, Long>> termLabelCounts = termCounts.groupBy(0).sum(2).project(0, 2).types(String.class, Long.class);

        termLabelCounts.writeAsCsv(Config.pathToSums(), "\n", "\t", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
            // normalize and split the line into words
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<String, Long>(token, 1L));
                }
            }
        }
    }

    public static class DataReader implements FlatMapFunction<String, Tuple3<String, String, Long>> {
        @Override
        public void flatMap(String line, Collector<Tuple3<String, String, Long>> collector) throws Exception {
            try {
                String[] tokens = line.split("\t");
                String player_name = tokens[0];
                String eventResult = tokens[1];
                String event = tokens[2];
                String arch = tokens[3];
                String[] terms = tokens[4].split("&&");

                String[] card;
                String name;
                Long nb;
                for (String term : terms) {
                    card = term.split("\\$\\$");
                    name = card[1];
                    nb = Long.parseLong(card[0]);
                    collector.collect(new Tuple3<String, String, Long>(arch, name, nb));
                }
            }catch(Exception e){
                System.out.println("Error, format problem: "+line);
            }
        }
    }
}
