package qengine.storage;

import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.io.IOException;
import java.util.*;


import static org.junit.jupiter.api.Assertions.assertEquals;

class RDFHexaStoreBenchmarkTest {
    @Test
    void testCorrectionCompletude100k() throws IOException {
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/100K.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");
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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/500K.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");
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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/2M.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");
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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/rdf/100K.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");

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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/500K.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");

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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/2M.nt");
        List<StarQuery> starQueries = Utils.parseSparQLQueries("data/queries/Q_4_location_nationality_gender_type.queryset");

        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        Utils.evaluationOfQueries(starQueries, factBase, store);
        Utils.speedTest(starQueries, store, factBase);
    }
    @Test
    void benchmark() throws IOException {
        HashMap<String, List<StarQuery>> datasetQueries = Utils.getQueriesFromDir("data/queries");
        HashMap<String, List<RDFAtom>> datasetRdf = Utils.getRDFFromDir("data/rdf");
        int count = 1;
        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()) {
            List<StarQuery> queries = entryQuery.getValue();
            queries = Utils.removeDups(queries).stream().toList();
            entryQuery.setValue(queries);
        }
        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()) {
            List<StarQuery> queries = entryQuery.getValue();
            String querysetName = entryQuery.getKey();
            for (Map.Entry<String, List<RDFAtom>> entryRdf: datasetRdf.entrySet()) {
                FactBase factBase = new SimpleInMemoryGraphStore();
                RDFHexaStore store = new RDFHexaStore();
                List<RDFAtom> rdfAtoms = entryRdf.getValue();
                String rdfName = entryRdf.getKey();
                rdfAtoms.stream().map(factBase::add);
                store.addAll(rdfAtoms);
                queries = removeNonMatching(queries, factBase, store);
                System.out.println("-------------------");

                System.out.println("Test#"+count);
                System.out.println("Dataset: "+ rdfName+", taille: "+ rdfAtoms.size());
                System.out.println("Queryset: "+ querysetName+ ", nb requête: "+ queries.size());

                Utils.speedTest(queries, store, factBase);
                count++;
            }
        }

    }
    @Test
    void preprocessing() throws IOException {

        HashMap<String, List<StarQuery>> datasetQueries = Utils.getQueriesFromDir("data/queries");
        HashMap<String, List<RDFAtom>> datasetRdf = Utils.getRDFFromDir("data/rdf");


        for (Map.Entry<String, List<StarQuery>> entryQuery: datasetQueries.entrySet()){
            String queryName = entryQuery.getKey();
            List<StarQuery> queries = entryQuery.getValue();

            System.out.println(queryName+": ");
            System.out.println("\tDoublons: "+ Utils.evaluateDups(queries)+"/"+queries.size());
            System.out.println("\tPourcentage: "+ (double) Utils.evaluateDups(queries)/queries.size());


            queries = Utils.removeDups(queries).stream().toList();
            assertEquals(0,Utils.evaluateDups(queries), "Failed to remove dups" );
            for (Map.Entry<String, List<RDFAtom>> entryRdf: datasetRdf.entrySet()){
                List<RDFAtom> rdfAtoms = entryRdf.getValue();
                String rdfAtomName = entryRdf.getKey();
                FactBase factBase = new SimpleInMemoryGraphStore();
                RDFHexaStore store = new RDFHexaStore();
                rdfAtoms.stream().map(factBase::add);
                store.addAll(rdfAtoms);

                int nbNonMatchingQueries = evaluatePercentilNonMatching(queries, store, factBase);
                System.out.println("\t\t"+rdfAtomName+":");

                System.out.println("\t\t\tNombre de requêtes vides: "+nbNonMatchingQueries+"/"+queries.size());
                System.out.println("\t\t\tPourcentage de requêtes vides: "+(double) nbNonMatchingQueries/queries.size());
                int nbQueriesRemoved = (int) (nbNonMatchingQueries * 0.05);
                int intialQueriesSize = queries.size();
                queries = removeNonMatching(queries, factBase, store);

                assertEquals(queries.size(), intialQueriesSize - nbNonMatchingQueries + nbQueriesRemoved, "Failed to remove non maching queries");
            }
        }



    }






    // Méthode pour évaluer les doublons et les requêtes sans correspondance
    int evaluatePercentilNonMatching(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
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


    // Méthode pour supprimer les requêtes sans correspondance avec les données
    List<StarQuery> removeNonMatching(List<StarQuery> queries, FactBase factBase, RDFHexaStore store) {
        // Liste pour stocker les requêtes non correspondantes
        List<StarQuery> nonMatchingQueries = new ArrayList<>();
        List<StarQuery> mergedQueries = new ArrayList<>();

        for (StarQuery query: queries){
            Iterator<Substitution> result = store.match(query);
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