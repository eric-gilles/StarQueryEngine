package qengine_concurrent.benchmark;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.parser.StarQuerySparQLParser;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class IntegraalBenchmark {

    public static void start(String dataFilePath, String querysetDirPath, String outputFilePath) throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(dataFilePath);

        FactBase factBase = new SimpleInMemoryGraphStore();
        for (RDFAtom atom : rdfAtoms) {
            factBase.add(atom);
        }

        System.out.println("Données RDF chargées dans Integraal. Début du benchmark...");

        Map<String, Long> results = executeGroupedQueries(querysetDirPath, factBase);
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

    private static Map<String, Long> executeGroupedQueries(String querySetDir, FactBase factBase) {
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

            List<FOQuery<FOFormulaConjunction>> foQueries = queries.stream().map(StarQuery::asFOQuery).collect(Collectors.toList());

            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();

            long startTime = System.currentTimeMillis();
            executeAllQueries(foQueries, evaluator, factBase);
            long totalTime = System.currentTimeMillis() - startTime;

            groupedResults.put(category, totalTime);
        }

        return groupedResults;
    }

    private static void executeAllQueries(List<FOQuery<FOFormulaConjunction>> foQueries, FOQueryEvaluator<FOFormula> evaluator, FactBase factBase) {
        for (FOQuery<FOFormulaConjunction> foQuery : foQueries) {
            evaluator.evaluate(foQuery, factBase);
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
