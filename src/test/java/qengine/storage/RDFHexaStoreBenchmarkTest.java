package qengine.storage;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.junit.jupiter.api.Test;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.io.File;
import java.io.IOException;
import java.util.*;

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
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/100K.nt");
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
    void preprocessing() throws IOException {
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        List<RDFAtom> rdfAtoms = Utils.parseRDFData("data/100K.nt");
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        benchmarkData(factBase, store);

        factBase = new SimpleInMemoryGraphStore();
        store = new RDFHexaStore();
        rdfAtoms = Utils.parseRDFData("data/500K.nt");
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        benchmarkData(factBase, store);

        factBase = new SimpleInMemoryGraphStore();
        store = new RDFHexaStore();
        rdfAtoms = Utils.parseRDFData("data/2M.nt");
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        benchmarkData(factBase, store);



    }

    void benchmarkData(FactBase factBase, RDFHexaStore store) throws IOException {

        // Traitement de tous les fichiers dans le répertoire "data/queries"
        File queriesDir = new File("data/queries");
        File[] queryFiles = queriesDir.listFiles((dir, name) -> name.endsWith(".queryset"));

        if (queryFiles != null) {
            for (File queryFile : queryFiles) {
                System.out.println("Processing file: " + queryFile.getName());

                // Charger les requêtes à partir du fichier actuel
                List<StarQuery> starQueries = Utils.parseSparQLQueries(queryFile.getPath());

                // Évaluer les doublons et les requêtes qui ne correspondent à aucun résultat
                evaluatePercentilDupsAndNonMatching(starQueries, store, factBase);

                // Suppression des doublons et des requêtes qui ne correspondent à rien
                removeDups(starQueries);
                removeNonMatching(starQueries, factBase, store);

            }
        } else {
            System.out.println("No query files found in the specified directory.");
        }
    }



    // Méthode pour évaluer les doublons et les requêtes sans correspondance
    void evaluatePercentilDupsAndNonMatching(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
        Set<StarQuery> queriesSet = new HashSet<>(queries);
        int nonMatchingQueries = 0;
        // Vérifier les requêtes qui ne correspondent à aucun résultat

        for (StarQuery query : queries) {
            FOQuery<FOFormulaConjunction> foQuery = query.asFOQuery(); // Conversion en FOQuery
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            Iterator<Substitution> subs = evaluator.evaluate(foQuery, factBase); // Évaluer la requête
            List<Substitution> subsList = new ArrayList<>();
            subs.forEachRemaining(subsList::add);
            if (subsList.isEmpty()) {
                nonMatchingQueries++;
            }
        }


        queriesSet.addAll(queries);

        // Calculer et afficher les pourcentages
        double dupPercentage =  (1- (queriesSet.size() / (double) queries.size())) * 100;
        double nonMatchingPercentage = (nonMatchingQueries/ (double) queries.size()) * 100;

        System.out.println("Dups: " + dupPercentage + "%");
        System.out.println("Non Matching Queries: " + nonMatchingPercentage + "%");
    }

    // Méthode pour supprimer les doublons dans la liste de requêtes
    void removeDups(List<StarQuery> queries) {
        for (int i = 0; i < queries.size(); i++) {
            for (int j = i + 1; j < queries.size(); j++) {
                if (queries.get(i).equals(queries.get(j))) {
                    queries.remove(j);
                    j--; // Recalcule l'index après la suppression
                }
            }
        }
    }

    // Méthode pour supprimer les requêtes sans correspondance avec les données
    void removeNonMatching(List<StarQuery> queries, FactBase factBase, RDFHexaStore store) {
        // Liste pour stocker les requêtes non correspondantes
        List<StarQuery> nonMatchingQueries = new ArrayList<>();

        // Identifier les requêtes non correspondantes
        Iterator<StarQuery> iterator = queries.iterator();
        while (iterator.hasNext()) {
            StarQuery query = iterator.next();
            Iterator<Substitution> subs = store.match(query);
            List<Substitution> subsList = new ArrayList<>();
            subs.forEachRemaining(subsList::add);
            if (subsList.isEmpty()) {
                nonMatchingQueries.add(query); // Ajouter à la liste des non-matching
                iterator.remove(); // Retirer la requête de la liste principale
            }
        }

        // Conserver 5 % des requêtes non correspondantes
        int numToKeep = (int) (nonMatchingQueries.size() * 0.05); // 5 % des requêtes non correspondantes à garder

        // Mélanger les requêtes non correspondantes pour choisir de manière aléatoire
        Random random = new Random();
        for (int i = 0; i < numToKeep; i++) {
            int randomIndex = random.nextInt(nonMatchingQueries.size());
            StarQuery queryToKeep = nonMatchingQueries.get(randomIndex);
            queries.add(queryToKeep); // Réajouter la requête non correspondante dans la liste des requêtes
            nonMatchingQueries.remove(randomIndex); // Supprimer de la liste des nonMatchingQueries
        }

        // Optionnel : afficher combien de requêtes ont été conservées
        System.out.println("Conservé 5% des requêtes non correspondantes. Nombre conservé : " + numToKeep);
    }

}