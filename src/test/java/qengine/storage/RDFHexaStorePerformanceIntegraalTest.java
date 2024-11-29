package qengine.storage;


import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests de performance pour comparer l'exécution de notre HexaStore avec Integraal.
 */
class RDFHexaStorePerformanceIntegraalTest {
    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE_SMALL = WORKING_DIR + "sample_data.nt";
    private static final String SAMPLE_DATA_FILE_BIG = WORKING_DIR + "100K.nt";

    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";
    private static final String SAMPLE_QUERY_FILE_ALL = WORKING_DIR + "STAR_ALL_workload.queryset";

    @Test
    void testCorrectionCompletude() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_SMALL);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        evaluationOfQueries(starQueries, factBase, store);

        rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_BIG);
        starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE_ALL);
        factBase = new SimpleInMemoryGraphStore();
        store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        evaluationOfQueries(starQueries, factBase, store);
    }

    private void evaluationOfQueries(List<StarQuery> queries, FactBase factBase, RDFHexaStore store){
        for (StarQuery starQuery : queries) {
            // Conversion en FOQuery
            FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery();
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            Iterator<Substitution> resIntegraal = evaluator.evaluate(foQuery, factBase);
            Iterator<Substitution> resHexa = store.match(starQuery);
            List<Substitution> listSubIntegraal = new ArrayList<>();
            List<Substitution> listSubHexastore = new ArrayList<>();
            resHexa.forEachRemaining(listSubHexastore::add);
            resIntegraal.forEachRemaining(listSubIntegraal::add);
            assertEquals(listSubIntegraal.size(), listSubHexastore.size(), "Not the same size");
            assertTrue(listSubHexastore.containsAll(listSubIntegraal), "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
            assertTrue(listSubIntegraal.containsAll(listSubHexastore), "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
        }
    }

    @Test
    void testSpeedOnBigDataset() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_BIG);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE_ALL);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        speedTest(starQueries, store, factBase);
    }

    @Test
    void testSpeedOnSmallDataset() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_SMALL);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        speedTest(starQueries, store, factBase);
    }

    private void speedTest(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
        // Mesurer le temps d'exécution de Integraal
        long startTimeIntegraal = System.currentTimeMillis();
        for (StarQuery starQuery : queries) {
            FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            evaluator.evaluate(foQuery, factBase); // Évaluer la requête
        }
        long endTimeIntegraal = System.currentTimeMillis();
        long durationIntegraal = endTimeIntegraal - startTimeIntegraal;

        // Mesurer le temps d'exécution de notre HexaStore
        long startTimeHexastore = System.currentTimeMillis();
        for(StarQuery starQuery: queries){
            store.match(starQuery);
        }
        long endTimeHexastore = System.currentTimeMillis();
        long durationHexastore = endTimeHexastore - startTimeHexastore;


        // Calculer les moyennes
        System.out.println("Average execution time of Integraal: " + (double) durationIntegraal / queries.size() + " ms");
        System.out.println("Average execution time of our HexaStore: " + (double) durationHexastore / queries.size() + " ms");
    }

    /**
     * Parse et affiche le contenu d'un fichier RDF.
     *
     * @param rdfFilePath Chemin vers le fichier RDF à parser
     * @return Liste des RDFAtoms parsés
     */
    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            int count = 0;
            while (rdfAtomParser.hasNext()) {
                RDFAtom atom = rdfAtomParser.next();
                rdfAtoms.add(atom);  // Stocker l'atome dans la collection
                // System.out.println("RDF Atom #" + (++count) + ": " + atom);
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
    private static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
        List<StarQuery> starQueries = new ArrayList<>();

        try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
            int queryCount = 0;

            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    starQueries.add(starQuery);  // Stocker la requête dans la collection
                    // System.out.println("Star Query #" + (++queryCount) + ":");
                    // System.out.println("  Central Variable: " + starQuery.getCentralVariable().label());
                    // System.out.println("  RDF Atoms:");
                    //starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
                } else {
                    // System.err.println("Requête inconnue ignorée.");
                }
            }
            System.out.println("Total Queries parsed: " + starQueries.size());
        }
        return starQueries;
    }
}
