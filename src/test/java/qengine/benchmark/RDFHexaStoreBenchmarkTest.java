package qengine.benchmark;


import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;

import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.storage.RDFHexaStore;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static qengine.benchmark.Utils.*;

class RDFHexaStoreBenchmarkTest {

    @Test
    void testCorrectionCompletude100k() throws IOException {
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_100K);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
    }

    @Test
    void testCorrectionCompletude500k() throws IOException {
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_500K);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
    }

    @Test
    void testCorrectionCompletude2M() throws IOException {
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_2M);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
    }

    @Test
    void testBenchmark100K() throws IOException {
        FactBase factBase = new SimpleInMemoryGraphStore();
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_100K);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);

        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
        Utils.speedTest(starQueries, store, factBase);
    }

    @Test
    void testBenchmark500K() throws IOException {
        FactBase factBase = new SimpleInMemoryGraphStore();
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_500K);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);

        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
        Utils.speedTest(starQueries, store, factBase);

    }

    @Test
    void testBenchmark2M() throws IOException {
        FactBase factBase = new SimpleInMemoryGraphStore();
        List<RDFAtom> rdfAtoms = Utils.parseRDFData(DATA_2M);
        List<StarQuery> starQueries = Utils.parseSparQLQueries(QUERIES_EXAMPLE);

        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
        Utils.speedTest(starQueries, store, factBase);
    }
    @Test
    void testPreprocessing() throws IOException {
        HashMap<String, List<StarQuery>> datasetQueries = Utils.getQueriesFromDir("data/queries");
        HashMap<String, List<RDFAtom>> datasetRdf = Utils.getRDFFromDir("data/rdf");


        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()){
            String queryName = entryQuery.getKey();
            List<StarQuery> queries = entryQuery.getValue();

            System.out.println(queryName+": ");
            System.out.println("\tDoublons: "+ Utils.evaluateDuplicateQueries(queries)+"/"+queries.size());
            System.out.println("\tPourcentage: "+ (double) Utils.evaluateDuplicateQueries(queries)/queries.size());


            queries = Utils.removeDuplicateQueries(queries).stream().toList();
            assertEquals(0,Utils.evaluateDuplicateQueries(queries), "Failed to remove dups" );
            for (Map.Entry<String, List<RDFAtom>> entryRdf: datasetRdf.entrySet()){
                List<RDFAtom> rdfAtoms = entryRdf.getValue();
                String rdfAtomName = entryRdf.getKey();
                FactBase factBase = new SimpleInMemoryGraphStore();
                RDFHexaStore store = new RDFHexaStore();
                rdfAtoms.stream().map(factBase::add);
                store.addAll(rdfAtoms);

                int nbNonMatchingQueries = evaluatePercentilNonMatching(queries, store);
                System.out.println("\t\t"+rdfAtomName+":");

                System.out.println("\t\t\tNombre de requêtes vides: "+nbNonMatchingQueries+"/"+queries.size());
                System.out.println("\t\t\tPourcentage de requêtes vides: "+(double) nbNonMatchingQueries/queries.size());
                int nbQueriesRemoved = (int) (nbNonMatchingQueries * 0.05);
                int intialQueriesSize = queries.size();
                queries = removeNonMatching(queries, factBase);

                assertEquals(queries.size(), intialQueriesSize - nbNonMatchingQueries + nbQueriesRemoved,
                        "Failed to remove non maching queries");
            }
        }
    }

    // Méthode pour évaluer les doublons et les requêtes sans correspondance
    int evaluatePercentilNonMatching(List<StarQuery> queries, RDFHexaStore store) {
        int nonMatchingQueries = 0;

        for (StarQuery query : queries) {
            Iterator<Substitution> subs = store.match(query);
            List<Substitution> subsList = new ArrayList<>();
            subs.forEachRemaining(subsList::add);
            if (subsList.isEmpty()) {
                nonMatchingQueries++;
            }
        }
        return nonMatchingQueries;
    }




    @Test
    void benchmark() throws IOException {
        HashMap<String, List<StarQuery>> datasetQueries = Utils.getQueriesFromDir(QUERIES_DIR);
        HashMap<String, List<RDFAtom>> datasetRdf = Utils.getRDFFromDir(DATA_DIR);

        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()) {
            List<StarQuery> queries = entryQuery.getValue();
            queries = Utils.removeDuplicateQueries(queries).stream().toList();
            entryQuery.setValue(queries);

        }

        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()){
            String queryName = entryQuery.getKey();
            List<StarQuery> queries = entryQuery.getValue();

            queries = Utils.removeDuplicateQueries(queries).stream().toList();
            for (Map.Entry<String, List<RDFAtom>> entryRdf: datasetRdf.entrySet()){
                List<RDFAtom> rdfAtoms = entryRdf.getValue();
                String rdfAtomName = entryRdf.getKey();
                FactBase factBase = new SimpleInMemoryGraphStore();
                RDFHexaStore store = new RDFHexaStore();
                rdfAtoms.stream().map(factBase::add);
                store.addAll(rdfAtoms);
                queries = removeNonMatching(queries, factBase);
                System.out.println("------------------------");
                System.out.println("Dataset: "+rdfAtomName);
                System.out.println("Query: "+ queryName+", size: "+ queries.size());
                Utils.speedTest(queries, store, factBase);

            }
        }
    }

}