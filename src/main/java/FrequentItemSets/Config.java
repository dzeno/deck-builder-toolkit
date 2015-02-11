package FrequentItemSets;

/**
 * @author Dzenan Softic
 * @author Tanguy Racinet
 */
public class Config {

    private static final String INPUT_PATH = "/Users/dzenansoftic/tub/deck-builder-toolkit/src/resources/";
    private static final String OUTPUT_PATH = "/tmp/deck-builder-toolkit/";

    private Config() {}

    public static String pathToTrainingSet() {
        return INPUT_PATH + "training_17K.txt";
    }

    public static String pathToTestSet() {
        return INPUT_PATH + "testing_7K.txt";
    }

    public static String pathToFrequentSets() {
        return OUTPUT_PATH + "sets";
    }

    public static String pathToRecommendations() {
        return OUTPUT_PATH + "recommendations";
    }

    public static String getArchetype() {
        return "UR Delver";
    }

    public static Double getSupportThreshold() {
        return  0.8;
    }

    public static Double getScoreThreshold() {
        return 0.7;
    }
}
