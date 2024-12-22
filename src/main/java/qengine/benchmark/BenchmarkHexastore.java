package qengine.benchmark;

import com.jayway.jsonpath.spi.cache.Cache;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Atom;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import jdk.jshell.execution.Util;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.OperatingSystem;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.storage.RDFHexaStore;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.CacheRequest;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.System.exit;
import static qengine.benchmark.Utils.*;

public class BenchmarkHexastore {

    public static void main(String[] args) throws IOException {
        // Default Query Directory to use for benchmarking modify it to change the query directory
        String queryDir = QUERIES_DIR_100;

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Benchmarking RDFHexaStore");
            System.out.println("Choose the RDF Data Set to use between : ");
            System.out.println("1. 100K");
            System.out.println("2. 500K");
            System.out.println("3. 2M");
            System.out.println("Enter the RDF Data Set to use : ");
            String choice = scanner.nextLine();
            switch (choice) {
                case "1" -> handlebenchmark(DATA_100K, queryDir, true);
                case "2" -> handlebenchmark(DATA_500K, queryDir, true);
                case "3" -> handlebenchmark(DATA_2M, queryDir, true);
                default -> {
                    System.out.println("Invalid choice");
                    exit(1);
                }
            }
        }
    }
    private static void resetCacheAndResources() {
        // Exemple : Réinitialisation des caches ou suppression de fichiers temporaires
        System.gc(); // Suggestion de nettoyage mémoire
    }

    private static void handlebenchmark(String dataset, String queryDir, Boolean prepocessing) throws IOException {
        System.out.println("## Benchmarking RDFHexaStore with " + dataset + " dataset ##\n\n");

        List<RDFAtom> rdfAtoms = Utils.parseRDFData(dataset);

        RDFHexaStore store = new RDFHexaStore(); // Benchmarking RDFHexaStore
        FactBase factBase = new SimpleInMemoryGraphStore(); // Benchmarking Integraal
        store.addAll(rdfAtoms);
        factBase.addAll(new HashSet<>(rdfAtoms));
        for (RDFAtom atom : rdfAtoms) {
            factBase.add(atom);
        }

        Map<String, Map<String, Long>> results = benchmark(store, factBase, queryDir, prepocessing);

        String benchmarkResultFile = saveBenchmarkResultsToFile(results, dataset);
        System.out.println("\n\n## Benchmarking Complete and Results saved in the file : " + benchmarkResultFile + " ##");
    }

    private static Map<String, Map<String, Long>> benchmark(
            RDFHexaStore store,
            FactBase factBase,
            String queriesDir,
            Boolean preprocessing
    ) throws IOException {
        Map<String, List<StarQuery>> queryFiles = Utils.getQueriesFromDir(queriesDir);
        if (preprocessing) {
            for (Map.Entry<String, List<StarQuery>> entryQuery: queryFiles.entrySet()) {
                List<StarQuery> queries = entryQuery.getValue();
                queries = Utils.removeDuplicateQueries(queries).stream().toList();
                entryQuery.setValue(queries);
            }
        }


        Map<String, Map<String, Long>> results = new HashMap<>();
        for (Map.Entry<String, List<StarQuery>> entryQuery: queryFiles.entrySet()) {
            List<StarQuery> starQueries = entryQuery.getValue();
            String nameFile = entryQuery.getKey();

            if(preprocessing) Utils.removeNonMatching(starQueries, factBase);

            System.out.println("Processing file: " + nameFile);
            String category = nameFile.split("_")[0] + nameFile.split("_")[1];

            Map<String, Long> durations = speedTest(starQueries, store, factBase);

            // Put the results in the map to save them in a file
            results.putIfAbsent(category, new HashMap<>());

            results.get(category).put("hexastore_" + nameFile, durations.get("Hexastore"));
            results.get(category).put("integraal_" + nameFile, durations.get("Integraal"));

        }
        return results;
    }

    private static String saveBenchmarkResultsToFile(Map<String, Map<String, Long>> results, String dataset) {
        // save benchmark results in a file
        LocalDateTime date = LocalDateTime.now();
        String formattedDate = date.format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss"));
        String formattedDataSetName = dataset.split("/")[2];
        String benchmarkResultFile = "data/benchmarks/benchmark_results_" + formattedDataSetName + "_" + formattedDate + ".txt";

        File file = new File("data/benchmarks");
        if (!file.exists()) {
            file.mkdirs();
        }

        try(FileWriter writer = new FileWriter(benchmarkResultFile)) {
            writer.append("### Benchmark Results ###\n");
            writer.append(getComputerInfo()).append("\n\n");

            writer.append("### HexaStore Results ###\n\n");
            long totalTimeHexaStore = 0;
            for (Map.Entry<String, Map<String, Long>> entry : results.entrySet()) {
                long categoryTimeHexastore = entry.getValue().entrySet().stream()
                        .filter(result -> result.getKey().startsWith("hexastore"))
                        .mapToLong(Map.Entry::getValue)
                        .sum();
                writer.append("Category: ").append(entry.getKey()).append("\t(Time: ").append(String.valueOf(categoryTimeHexastore)).append(" ms)\n");
                for (Map.Entry<String, Long> result : entry.getValue().entrySet()) {
                    Long value = result.getValue();
                    if (result.getKey().startsWith("hexastore") && value != null) {
                        String fileName = result.getKey().substring("hexastore_".length());
                        totalTimeHexaStore += value;
                        writer.append(fileName).append(": ").append(value.toString()).append(" ms\n");
                    }
                }
                writer.append("\n\n");
            }
            writer.append("# HexaStore Total Time: ").append(String.valueOf(totalTimeHexaStore)).append(" ms\n\n\n");

            writer.append("### Integraal Results ###\n\n");
            long totalTimeIntegraal = 0;
            for (Map.Entry<String, Map<String, Long>> entry : results.entrySet()) {
                long categoryTimeIntegraal = entry.getValue().entrySet().stream()
                        .filter(result -> result.getKey().startsWith("integraal"))
                        .mapToLong(Map.Entry::getValue)
                        .sum();
                writer.append("Category: ").append(entry.getKey()).append("\t(Time: ").append(String.valueOf(categoryTimeIntegraal)).append(" ms)\n");
                for (Map.Entry<String, Long> result : entry.getValue().entrySet()) {
                    if (result.getKey().startsWith("integraal") && result.getValue() != null) {
                        String fileName = result.getKey().substring("integraal_".length());
                        totalTimeIntegraal += result.getValue();
                        writer.append(fileName).append(": ").append(result.getValue().toString()).append(" ms\n");
                    }
                }
                writer.append("\n");
            }
            writer.append("# Integraal Total Time: ").append(String.valueOf(totalTimeIntegraal)).append(" ms\n\n");

        }catch (IOException exception){
            System.err.println("Error while saving benchmark results to file : " + exception.getMessage());
        }

        return benchmarkResultFile;
    }


    public static String getComputerInfo() {
        StringBuilder infos = new StringBuilder();
        SystemInfo systemInfo = new SystemInfo();
        HardwareAbstractionLayer hardware = systemInfo.getHardware();
        OperatingSystem os = systemInfo.getOperatingSystem();

        // Operating System Information
        infos.append("\n### System Information ###\n");
        infos.append("Operating System: ").append(os).append("\n");

        // CPU Information
        CentralProcessor processor = hardware.getProcessor();
        infos.append("CPU: ").append(processor).append("\n");

        // Memory Information
        GlobalMemory memory = hardware.getMemory();
        infos.append("Total Memory: ").append(memory.getTotal() / (1024 * 1024 * 1024)).append(" GB\n");
        infos.append("Available Memory: ").append(memory.getAvailable() / (1024 * 1024 * 1024)).append(" GB\n");

        // Java Information
        infos.append("Java Version: ").append(System.getProperty("java.version")).append("\n");
        infos.append("Java Vendor: ").append(System.getProperty("java.vendor")).append("\n");
        infos.append("Java VM: ").append(System.getProperty("java.vm.name")).append("\n");

        return infos.toString();
    }
}
