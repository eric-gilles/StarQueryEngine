package qengine_concurrent.program;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import qengine_concurrent.benchmark.HexastoreBenchmark;
import qengine_concurrent.benchmark.HexastoreConcurentBenchmark;
import qengine_concurrent.benchmark.IntegraalBenchmark;

import java.util.List;
import java.util.concurrent.Callable;

@Command(name = "qbt", mixinStandardHelpOptions = true, version = "qbt 0.0.1", description = "Qengine Benchmarking Tool.")
class QengineBenchmarkingTool implements Callable<Integer> {

    @Option(names = {"-d", "--data"}, required = true, description = "The path of the data file.")
    private String dataFilePath;

    @Option(names = {"-q", "--queryset"}, required = true, description = "The path of the queryset directory.")
    private String querysetDirPath;

    @Option(names = {"-o", "--output"}, required = true, description = "The path of the output file.")
    private String outputFilePath;

    @Option(names = {"-i", "--implementation"}, required = true, description = "The implementation to use: hexastore, integraal")
    private String implementation;

    @Override
    public Integer call() throws Exception {
        if (!List.of("hexastore", "integraal", "concurrent").contains(implementation)) {
            System.out.println("Invalid implementation: " + implementation);
            return 1;
        }
        System.out.println("Welcome to Qengine Benchmarking Tool!");
        System.out.println("You chose the " + implementation + " implementation.");
        System.out.println("Data file path: " + dataFilePath);
        System.out.println("Query file path: " + querysetDirPath);
        if (implementation.equals("hexastore")) {
            HexastoreBenchmark.start(dataFilePath, querysetDirPath, outputFilePath);
        } else if (implementation.equals("integraal")) {
            IntegraalBenchmark.start(dataFilePath, querysetDirPath, outputFilePath);
        } else if (implementation.equals("concurrent")) {
            HexastoreConcurentBenchmark.start(dataFilePath, querysetDirPath, outputFilePath);
        }
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new QengineBenchmarkingTool()).execute(args);
        System.exit(exitCode);
    }
}
