package qengine.storage;


import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class Utils {


        static void speedTest(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
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
            System.out.println("Execution time of Integraal: " + (double) durationIntegraal + " ms");
            System.out.println("Execution time of our HexaStore: " + (double) durationHexastore + " ms");
        }

        /**
         * Parse et affiche le contenu d'un fichier RDF.
         *
         * @param rdfFilePath Chemin vers le fichier RDF à parser
         * @return Liste des RDFAtoms parsés
         */
        static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
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
        static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
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

    static void evaluationOfQueries(List<StarQuery> queries, FactBase factBase, RDFHexaStore store){
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
            assertTrue(listSubHexastore.containsAll(listSubIntegraal), "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
            assertTrue(listSubIntegraal.containsAll(listSubHexastore), "Missing substitutions: "+listSubHexastore+" : "+listSubIntegraal );
        }
    }

    static HashMap<String,List<StarQuery>> getQueriesFromDir(String dirname) throws IOException {
        HashMap<String,List<StarQuery>> hashMap = new HashMap<>();
        File queriesDir = new File(dirname);
        File[] queryFiles = queriesDir.listFiles((dir, name) -> name.endsWith(".queryset"));

        if (queryFiles != null) {
            for (File queryFile : queryFiles) {
                // Charger les requêtes à partir du fichier actuel
                List<StarQuery> starQueries = parseSparQLQueries(queryFile.getPath());
                if (starQueries.isEmpty()) continue;
                hashMap.put(queryFile.getName(), starQueries);
            }
        }
        return hashMap;
    }

    static HashMap<String,List<RDFAtom>> getRDFFromDir(String dirname) throws IOException {
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
    static int evaluateDups(List<StarQuery> queries){
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
    static Set<StarQuery> removeDups(List<StarQuery> queries) {
        return new HashSet<>(queries);
    }


}
