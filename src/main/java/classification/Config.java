package classification;

public class Config {
  private static final String INPUT_PATH = "/Users/dzenansoftic/tub/deck-builder-toolkit/src/main/resources/";
  private static final String OUTPUT_PATH = "/tmp/deck-builder-toolkit/";

  private Config() {}

  public static String pathToTrainingSet() {
    return INPUT_PATH + "training_17K.txt";
  }

  public static String pathToTestSet() {
    return INPUT_PATH + "testing_7K.txt";
  }

  public static String pathToOutput() {
    return OUTPUT_PATH + "result";
  }

  public static String pathToSums() {
    return OUTPUT_PATH + "sums";
  }

  public static String pathToConditionals() {
    return OUTPUT_PATH + "conditionals";
  }

  public static Long getSmoothingParameter() {
    return 1L;
  }
}
