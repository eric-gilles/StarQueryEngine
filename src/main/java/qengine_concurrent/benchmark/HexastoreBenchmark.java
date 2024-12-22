package qengine_concurrent.benchmark;

import org.eclipse.rdf4j.rio.RDFFormat;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.parser.StarQuerySparQLParser;
import qengine_concurrent.storage.RDFHexaStore;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class HexastoreBenchmark {

    public static void start(String dataFilePath, String querysetDirPath, String outputFilePath) throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(dataFilePath);

        RDFHexaStore store = new RDFHexaStore();
        store.addAll(rdfAtoms);

        System.out.println("Données RDF chargées dans le HexaStore. Début du benchmark...");

        Map<String, Long> results = executeGroupedQueries(querysetDirPath, store);
        saveResultsToFile(results, outputFilePath);

        System.out.println("Benchmark terminé. Résultats enregistrés dans le répertoire " + outputFilePath + ".");
    }

    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            while (rdfAtomParser.hasNext()) {
                rdfAtoms.add(rdfAtomParser.next());
            }
        }
        return rdfAtoms;
    }

    private static Map<String, Long> executeGroupedQueries(String querySetDir, RDFHexaStore store) {
        Map<String, Long> groupedResults = new TreeMap<>();
        File dir = new File(querySetDir);

        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Le répertoire spécifié n'existe pas : " + querySetDir);
            return groupedResults;
        }

        File[] queryFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (queryFiles == null) {
            System.err.println("Aucun fichier queryset trouvé dans : " + querySetDir);
            return groupedResults;
        }

        Map<String, List<File>> groupedFiles = new TreeMap<>();
        for (File queryFile : queryFiles) {
            String fileName = queryFile.getName();
            String[] parts = fileName.split("_");
            String category = parts[0] + parts[1];

            groupedFiles.computeIfAbsent(category, k -> new ArrayList<>()).add(queryFile);
        }

        for (Map.Entry<String, List<File>> entry : groupedFiles.entrySet()) {
            String category = entry.getKey();
            List<File> files = entry.getValue();

            List<StarQuery> queries = new ArrayList<>();
            for (File file : files) {
                queries.addAll(loadAllQueriesFromFile(file));
            }

            long startTime = System.currentTimeMillis();
            executeAllQueries(queries, store);
            long totalTime = System.currentTimeMillis() - startTime;

            groupedResults.put(category, totalTime);
        }

        return groupedResults;
    }

    private static void executeAllQueries(List<StarQuery> queries, RDFHexaStore store) {
        for (StarQuery query : queries) {
            store.match(query);
        }
    }

    private static List<StarQuery> loadAllQueriesFromFile(File queryFile) {
        List<StarQuery> queries = new ArrayList<>();
        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(queryFile.getAbsolutePath())) {
            while (parser.hasNext()) {
                StarQuery query = (StarQuery) parser.next();
                queries.add(query);
            }
        } catch (IOException e) {
            System.err.println("Erreur lors du traitement du fichier : " + queryFile.getName());
        }
        return queries;
    }

    private static void saveResultsToFile(Map<String, Long> results, String outputFilePath) {
        try (FileWriter writer = new FileWriter(outputFilePath)) {
            writer.write("=== MACHINE ===\n");
            writer.write(MachineInfo.getMachineInfo());
            writer.write("\n");

            for (Map.Entry<String, Long> entry : results.entrySet()) {
                writer.write("=== " + entry.getKey() + " ===\n");
                writer.write("TOTAL : " + entry.getValue() + "ms\n\n");
            }
            System.out.println("Résultats sauvegardés dans : " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture du fichier de benchmark : " + outputFilePath);
        }
    }

}
