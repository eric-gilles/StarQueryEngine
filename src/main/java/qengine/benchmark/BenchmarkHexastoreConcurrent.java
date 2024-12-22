package qengine.benchmark;

import org.eclipse.rdf4j.rio.RDFFormat;
import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import qengine.parser.StarQuerySparQLParser;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.storage.RDFHexaStore;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static qengine.benchmark.Utils.QUERIES_DIR_100;
import static qengine.benchmark.Utils.getFilesFromDir;

import java.io.File;

public class BenchmarkHexastoreConcurrent {

    public static void main(String[] args) throws IOException {
        // Default Query Directory to use for benchmarking modify it to change the query directory
        String queryDir = QUERIES_DIR_100;

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Benchmarking Concurrent RDFHexaStore");
            System.out.println("Choose the RDF Data Set to use between : ");
            System.out.println("1. 100K");
            System.out.println("2. 500K");
            System.out.println("3. 2M");
            System.out.println("Enter the RDF Data Set to use : ");
            String choice = scanner.nextLine();
            System.out.println("Do you want to enable preprocessing? (yes/no): ");
            String preprocessingChoice = scanner.nextLine();
            Boolean preprocessing = preprocessingChoice.equalsIgnoreCase("yes")
                    || preprocessingChoice.equalsIgnoreCase("y");

            switch (choice) {
                case "1" -> handleBenchmark("data/datasets/100K.nt", queryDir, preprocessing);
                case "2" -> handleBenchmark("data/datasets/500K.nt", queryDir, preprocessing);
                case "3" -> handleBenchmark("data/datasets/2M.nt", queryDir, preprocessing);
                default -> {
                    System.out.println("Invalid choice");
                    System.exit(1);
                }
            }
        }
    }

    private static void handleBenchmark(String dataset, String queryDir, Boolean preprocessing) throws IOException {
        System.out.println("## Benchmarking Concurrent RDFHexaStore with " + dataset + " dataset ##\n\n");

        List<RDFAtom> rdfAtoms = parseRDFData(dataset);

        RDFHexaStore store = new RDFHexaStore(); // Benchmarking Concurrent RDFHexaStore
        FactBase factBase = new SimpleInMemoryGraphStore(); // Benchmarking Integraal
        store.addAll(rdfAtoms);
        factBase.addAll(new HashSet<>(rdfAtoms));

        Map<String, Map<String, Long>> results = benchmark(store, factBase, queryDir, preprocessing);

        String benchmarkResultFile = saveBenchmarkResultsToFile(results, dataset);
        System.out.println("\n\n## Benchmarking Complete and Results saved in the file : " + benchmarkResultFile + " ##");
    }

    private static Map<String, Map<String, Long>> benchmark(RDFHexaStore store, FactBase factbase, String queriesDir, Boolean preprocessing) throws IOException {
        Map<String, List<StarQuery>> queryFiles = getQueriesFromDir(queriesDir);
        if (preprocessing) {
            for (Map.Entry<String, List<StarQuery>> entryQuery : queryFiles.entrySet()) {
                List<StarQuery> queries = entryQuery.getValue();
                queries = removeDuplicateQueries(queries).stream().toList();
                entryQuery.setValue(queries);
            }
        }

        Map<String, Map<String, Long>> results = new HashMap<>();
        for (Map.Entry<String, List<StarQuery>> entryQuery : queryFiles.entrySet()) {
            List<StarQuery> starQueries = entryQuery.getValue();
            String nameFile = entryQuery.getKey();

            if (preprocessing) removeNonMatching(starQueries, factbase);

            System.out.println("Processing file: " + nameFile);
            String category = nameFile.split("_")[0] + nameFile.split("_")[1];

            Map<String, Long> durations = speedTest(starQueries, store, null);

            // Put the results in the map to save them in a file
            results.putIfAbsent(category, new HashMap<>());

            results.get(category).put("concurrent_hexastore_" + nameFile, durations.get("ConcurrentHexastore"));
        }
        return results;
    }


    public static Map<String, Long> speedTest(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
        Map<String, Long> durations = new HashMap<>();

        // Mesurer le temps d'exécution de Integraal
        long startTimeIntegraal = System.currentTimeMillis();
        for (StarQuery starQuery : queries) {
            FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            evaluator.evaluate(foQuery, factBase); // Évaluer la requête
        }
        long endTimeIntegraal = System.currentTimeMillis();
        long durationIntegraal = endTimeIntegraal - startTimeIntegraal;
        if (durationIntegraal == 0) durationIntegraal = 1;
        durations.put("Integraal", durationIntegraal);

        // Mesurer le temps d'exécution de notre HexaStore
        long startTimeHexastore = System.currentTimeMillis();
        for(StarQuery starQuery: queries){
            store.match(starQuery);
        }
        long endTimeHexastore = System.currentTimeMillis();
        long durationHexastore = endTimeHexastore - startTimeHexastore;
        if (durationHexastore == 0) durationHexastore = 1;

        durations.put("Hexastore", durationHexastore);

        // Calculer les moyennes
        System.out.println("Execution time of Integraal: " + (double) durationIntegraal + " ms");
        System.out.println("Execution time of our HexaStore: " + (double) durationHexastore + " ms");

        return durations;
    }

    public static HashMap<String, List<StarQuery>> getQueriesFromDir(String queriesDir) throws IOException {
        HashMap<String,List<StarQuery>> hashMap = new HashMap<>();
        File[] queryFiles = getFilesFromDir(queriesDir);

        for (File queryFile : queryFiles) {
            // Charger les requêtes à partir du fichier actuel
            List<StarQuery> starQueries = parseSparQLQueries(queryFile.getPath());
            if (starQueries.isEmpty()) continue;
            hashMap.put(queryFile.getName(), starQueries);
        }
        return hashMap;
    }

    /**
     * Parse et affiche le contenu d'un fichier RDF.
     *
     * @param rdfFilePath Chemin vers le fichier RDF à parser
     * @return Liste des RDFAtoms parsés
     */
    public static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            int count = 0;
            while (rdfAtomParser.hasNext()) {
                RDFAtom atom = rdfAtomParser.next();
                rdfAtoms.add(atom);  // Stocker l'atome dans la collection
                count++;
            }
            System.out.println("Total RDF Atoms parsed: " + count);
        }
        return rdfAtoms;
    }

    /**
     * Parse et affiche le contenu d'un fichier de requêtes SparQL.
     *
     * @param queryFilePath Chemin vers le fichier de requêtes SparQL
     * @return Liste des StarQueries parsées
     */
    public static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
        List<StarQuery> starQueries = new ArrayList<>();

        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    starQueries.add(starQuery);  // Stocker la requête dans la collection
                }
            }
            System.out.println("Total Queries parsed: " + starQueries.size());
            return starQueries;
        }
    }

    // Méthode pour supprimer les doublons dans la liste de requêtes
    public static Set<StarQuery> removeDuplicateQueries(List<StarQuery> queries) {
        return new HashSet<>(queries);
    }

    // Méthode pour supprimer les requêtes sans correspondance avec les données
    public static List<StarQuery> removeNonMatching(List<StarQuery> queries, FactBase factBase) {
        // Liste pour stocker les requêtes non correspondantes
        List<StarQuery> nonMatchingQueries = new ArrayList<>();
        List<StarQuery> mergedQueries = new ArrayList<>();

        for (StarQuery query: queries){
            FOQuery<FOFormulaConjunction> foQuery = query.asFOQuery();
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            Iterator<Substitution> result = evaluator.evaluate(foQuery, factBase);
            List<Substitution> listSub = new ArrayList<>();
            result.forEachRemaining(listSub::add);
            if (listSub.isEmpty()){
                nonMatchingQueries.add(query);
            } else {
                mergedQueries.add(query);
            }
        }
        // Conserver 5 % des requêtes non correspondantes
        int numToKeep = (int) (nonMatchingQueries.size() * 0.05); // 5 % des requêtes non correspondantes à garder

        // Mélanger les requêtes non correspondantes pour choisir de manière aléatoire
        Random random = new Random();
        for (int i = 0; i < numToKeep; i++) {
            int randomIndex = random.nextInt(nonMatchingQueries.size());
            StarQuery queryToKeep = nonMatchingQueries.get(randomIndex);
            mergedQueries.add(queryToKeep);
            nonMatchingQueries.remove(queryToKeep);
        }
        return mergedQueries;
    }

    /**
     * Save the benchmark results to a file
     * @param results Map of results for each query
     * @param dataset Dataset used for benchmarking
     * @return Path of the file where the results are saved
     */
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
            writer.append(Utils.getComputerInfo()).append("\n\n");

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
}