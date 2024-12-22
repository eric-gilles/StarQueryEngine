package qengine_concurrent.program;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.parser.StarQuerySparQLParser;
import qengine_concurrent.storage.RDFHexaStore;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class IntegraalSoundComplete {

    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE = WORKING_DIR + "100K.nt";
    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "STAR_ALL_workload.queryset";

    public static void main(String[] args) throws IOException {
        System.out.println("Parsing RDF Data");
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

        System.out.println("Parsing Sample Queries");
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);

        FactBase factBase = new SimpleInMemoryGraphStore();
        for (RDFAtom atom : rdfAtoms) {
            factBase.add(atom);
        }

        RDFHexaStore store = new RDFHexaStore();
        store.addAll(rdfAtoms);

        int passedTests = 0;
        int failedTests = 0;

        System.out.println("Executing and comparing the results of the queries with both Integraal and our implementation");
        for (StarQuery starQuery : starQueries) {
            boolean isTheSame = executeStarQuery(starQuery, factBase, store);
            if (isTheSame) {
//                System.out.println("PASSED");
                passedTests++;
            } else {
//                System.out.println("FAILED");
                failedTests++;
            }
        }
        System.out.println();
        System.out.println("Soundness & Completeness Results:");
        System.out.println("|_ Passed: " + passedTests);
        System.out.println("|_ Failed: " + failedTests);
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
                count++;
//                System.out.println("RDF Atom #" + count + ": " + atom);
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
//                    System.out.println("Star Query #" + (++queryCount) + ":");
//                    System.out.println("  Central Variable: " + starQuery.getCentralVariable().label());
//                    System.out.println("  RDF Atoms:");
//                    starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
                } else {
//                    System.err.println("Requête inconnue ignorée.");
                }
            }
            System.out.println("Total Queries parsed: " + starQueries.size());
        }
        return starQueries;
    }

    /**
     * Exécute une requête en étoile sur le store et affiche les résultats.
     *
     * @param starQuery La requête à exécuter
     * @param factBase  Le store contenant les atomes
     * @return L'égalité entre les résultats de Integraal et de notre implémentation
     */
    private static boolean executeStarQuery(StarQuery starQuery, FactBase factBase, RDFHexaStore store) {
        Iterator<Substitution> queryResults;
        List<Substitution> integraalSubstitutions = new ArrayList<>();
        List<Substitution> ourImplementationSubstitutions = new ArrayList<>();
//        System.out.printf("Execution of  %s:%n", starQuery);

//        System.out.println("With Integraal:");
        FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
        FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance(); // Créer un évaluateur
        queryResults = evaluator.evaluate(foQuery, factBase); // Évaluer la requête
//        System.out.println("Answers:");
//        if (!queryResults.hasNext()) {
//            System.out.println("No answer.");
//        }
        while (queryResults.hasNext()) {
            Substitution result = queryResults.next();
            integraalSubstitutions.add(result);
//            System.out.println(result); // Afficher chaque réponse
        }
//        System.out.println();

//        System.out.println("With our implementation:");
        queryResults = store.match(starQuery);
//        System.out.println("Answers:");
//        if (!queryResults.hasNext()) {
//            System.out.println("No answer.");
//        }
        while (queryResults.hasNext()) {
            Substitution result = queryResults.next();
            ourImplementationSubstitutions.add(result);
//            System.out.println(result); // Afficher chaque réponse
        }
//        System.out.println();

        return integraalSubstitutions.containsAll(ourImplementationSubstitutions) && ourImplementationSubstitutions.containsAll(integraalSubstitutions) && integraalSubstitutions.size() == ourImplementationSubstitutions.size();
    }

}
