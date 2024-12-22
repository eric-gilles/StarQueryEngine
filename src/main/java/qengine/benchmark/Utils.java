package qengine.benchmark;


import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Utils {
    // DataSets Files
    static final String DATA_DIR = "data/datasets/";
    public static final String DATA_100K = DATA_DIR + "100K.nt";
    public static final String DATA_500K = DATA_DIR + "500K.nt";
    public static final String DATA_2M = DATA_DIR + "2M.nt";

    // QueriesSets Files
    public static final String QUERIES_EXAMPLE = "data/queries/Q_4_location_nationality_gender_type.queryset";
    static final String QUERIES_DIR = "watdiv-mini-projet/testsuite/queries/";
    static final String QUERIES_DIR_100 = QUERIES_DIR + "100/";
    static final String QUERIES_DIR_1000 = QUERIES_DIR + "1000/";
    static final String QUERIES_DIR_10000 = QUERIES_DIR + "10000/";

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

    public static void evaluationOfQueries(List<StarQuery> queries, FactBase factBase, RDFHexaStore store){
        int count = 0;
        for (StarQuery starQuery : queries) {
            count++;
            System.out.println("Star Query #" + count );
            FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery();
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            Iterator<Substitution> resIntegraal = evaluator.evaluate(foQuery, factBase);
            Iterator<Substitution> resHexa = store.match(starQuery);
            List<Substitution> listSubIntegraal = new ArrayList<>();
            List<Substitution> listSubHexastore = new ArrayList<>();
            resHexa.forEachRemaining(listSubHexastore::add);
            resIntegraal.forEachRemaining(listSubIntegraal::add);
            assertEquals(listSubIntegraal.size(), listSubHexastore.size(), "Not the same size");
            assertTrue(new HashSet<>(listSubHexastore).containsAll(listSubIntegraal),
                    "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
            assertTrue(new HashSet<>(listSubIntegraal).containsAll(listSubHexastore),
                    "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
        }
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

    public static HashMap<String, List<RDFAtom>> getRDFFromDir(String dirname) throws IOException {
        HashMap<String,List<RDFAtom>> hashMap = new HashMap<>();
        File rdfFir = new File(dirname);
        File[] rdfFiles = rdfFir.listFiles((dir, name) -> name.endsWith(".nt"));

        if (rdfFiles != null) {
            for (File rdfFile : rdfFiles) {
                // Charger les requêtes à partir du fichier actuel
                List<RDFAtom> rdfAtoms = parseRDFData(rdfFile.getPath());
                if (rdfAtoms.isEmpty()) continue;
                hashMap.put(rdfFile.getName(), rdfAtoms);
            }
        }
        return hashMap;
    }
    public static int evaluateDuplicateQueries(List<StarQuery> queries){
        int count = 0;
        List<StarQuery> alreadyDone = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
            if(alreadyDone.contains(queries.get(i))) continue;
            for (int j = i + 1; j < queries.size(); j++) {
                if (queries.get(i).equals(queries.get(j))) count++;
            }
            alreadyDone.add(queries.get(i));
        }
        return count;
    }

    // Méthode pour supprimer les doublons dans la liste de requêtes
    public static Set<StarQuery> removeDuplicateQueries(List<StarQuery> queries) {
        return new HashSet<>(queries);
    }

    // Méthode pour récupérer les fichiers de requêtes à partir d'un répertoire
    public static File[] getFilesFromDir(String queriesDir) {
        File dir = new File(queriesDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("The directory " + queriesDir + " does not exist or is not a directory");
            return new File[0];
        }

        File[] queryFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (queryFiles == null || queryFiles.length == 0) {
            System.err.println("No query files .queryset found in the directory " + queriesDir);
            return new File[0];
        }
        return queryFiles;
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
}