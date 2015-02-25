package classification;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @author Dzenan Softic
 * @author Tanguy Racinet
 */
public class Evaluator {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read output from classification
        DataSource<String> predictions = env.readTextFile(Config.pathToOutput());
		//	restructure classification results
        DataSet<Tuple3<String, String, Double>> classifiedDataPoints = predictions.map(new ConditionalReader());
		// evaluate predictions accuracy
        DataSet<String> evaluation = classifiedDataPoints.reduceGroup(new Evaluate());

        evaluation.print();

        env.execute();
    }

    public static class ConditionalReader implements MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String line) throws Exception {
            String[] elements = line.split("\t");
            return new Tuple3<String, String, Double>(elements[0], elements[1], Double.parseDouble(elements[2]));
        }
    }

    public static class Evaluate implements GroupReduceFunction<Tuple3<String, String, Double>, String> {

        double correct = 0;
        double total = 0;

        @Override
        public void reduce(Iterable<Tuple3<String, String, Double>> predictions, Collector<String> collector)
                throws Exception {

            for(Tuple3<String, String, Double> prediction : predictions){
                if (prediction.f0.equals(prediction.f1)) correct++;
                total++;
            }
            double accuracy = correct/total * 100;
            collector.collect("Classifier achieved: " + (int)accuracy + " % accuracy");
        }
    }
}
