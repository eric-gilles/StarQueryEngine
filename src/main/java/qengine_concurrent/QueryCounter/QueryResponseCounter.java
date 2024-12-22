package qengine_concurrent.QueryCounter;

import com.google.common.collect.Iterators;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.*;
import java.util.*;

public class QueryResponseCounter {

    private static final String QUERYSET_DIR = "watdiv-mini-projet-partie-2/testsuite/queries/";
    private static final String OUTPUT_FILE_500K = "query_counter/query_responses_500k.dat";
    private static final String OUTPUT_FILE_2M = "query_counter/query_responses_2m.dat";

    public static void main(String[] args) {

        // === 500K ===

        RDFHexaStore storeWith500K = new RDFHexaStore();

        String rdfFilePathWith500K = "data/500K.nt";

        // Charger les données RDF dans le moteur
        loadRDFData(rdfFilePathWith500K, storeWith500K);

        // Calculer le nombre de réponses pour chaque requête
        calculateQueryResponses(QUERYSET_DIR, storeWith500K, OUTPUT_FILE_500K);

        System.out.println("Calcul terminé. Les résultats ont été sauvegardés dans le fichier : " + OUTPUT_FILE_500K);

        keepFivePercentOfZeroResponse(OUTPUT_FILE_500K);

        System.out.println("5% des réponses 0 ont été conservées dans le fichier : " + OUTPUT_FILE_500K);

        // === 2M ===

        RDFHexaStore storeWith2M = new RDFHexaStore();

        String rdfFilePathWith2M = "data/2M.nt";

        loadRDFData(rdfFilePathWith2M, storeWith2M);

        calculateQueryResponses(QUERYSET_DIR, storeWith2M, OUTPUT_FILE_2M);

        System.out.println("Calcul terminé. Les résultats ont été sauvegardés dans le fichier : " + OUTPUT_FILE_2M);

        keepFivePercentOfZeroResponse(OUTPUT_FILE_2M);

        System.out.println("5% des réponses 0 ont été conservées dans le fichier : " + OUTPUT_FILE_2M);

        System.out.println("=== Tous les calculs sont terminés. ===");


    }


    /**
     * Parse le contenu d'un fichier RDF.
     *
     * @param rdfFilePath Chemin vers le fichier RDF à parser
     * @return Liste des RDFAtoms parsés
     */
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

    /**
     * Charge les données RDF dans le RDFHexaStore.
     *
     * @param rdfFilePath Chemin vers le fichier RDF
     * @param store       Instance du RDFHexaStore
     */
    private static void loadRDFData(String rdfFilePath, RDFHexaStore store) {
        try {
            List<RDFAtom> rdfAtoms = parseRDFData(rdfFilePath);
            store.addAll(rdfAtoms);
            System.out.println("Données RDF chargées depuis : " + rdfFilePath);
        } catch (IOException e) {
            System.err.println("Erreur lors du chargement des données RDF : " + e.getMessage());
        }
    }

    /**
     * Parcourt les fichiers `.queryset`, exécute chaque requête, et enregistre le nombre de réponses dans un fichier unique.
     *
     * @param querySetDir Chemin vers le répertoire contenant les fichiers `.queryset`
     * @param store       Instance du RDFHexaStore
     */
    private static void calculateQueryResponses(String querySetDir, RDFHexaStore store, String outputFilePath) {
        File dir = new File(querySetDir);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Le répertoire spécifié n'existe pas : " + querySetDir);
            return;
        }

        File[] queryFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (queryFiles == null) {
            System.err.println("Aucun fichier queryset trouvé dans : " + querySetDir);
            return;
        }

        File outputFile = new File(outputFilePath);

        // Créer le répertoire si nécessaire
        if (!outputFile.getParentFile().exists()) {
            outputFile.getParentFile().mkdirs();
        }

        try (FileWriter writer = new FileWriter(outputFile)) {
            for (File queryFile : queryFiles) {
                try (StarQuerySparQLParser parser = new StarQuerySparQLParser(queryFile.getAbsolutePath())) {
                    Set<StarQuery> uniqueQueries = new HashSet<>(); // Stocke les requêtes uniques
                    while (parser.hasNext()) {
                        StarQuery query = (StarQuery) parser.next();
                        if (uniqueQueries.add(query)) { // Ajoute seulement si elle est unique
                            long responseCount = Iterators.size(store.match(query));
                            writer.write(responseCount + "\n");
                        }
                    }
                }
            }
            System.out.println("Résultats écrits dans : " + outputFile.getAbsolutePath());
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture du fichier : " + outputFilePath);
        }
    }

    /**
     * Garde 5% des réponses 0 dans un fichier .dat.
     *
     * @param pathFile Chemin vers le fichier contenant les résultats des réponses
     */
    public static void keepFivePercentOfZeroResponse(String pathFile) {
        File inputFile = new File(pathFile);
        File tempFile = new File(inputFile.getParent(), "filtered_" + inputFile.getName());

        List<String> nonZeroResponses = new ArrayList<>();
        List<String> zeroResponses = new ArrayList<>();

        // Lire les données du fichier existant
        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals("0")) {
                    zeroResponses.add(line);
                } else {
                    nonZeroResponses.add(line);
                }
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de la lecture du fichier : " + e.getMessage());
            return;
        }

        // Calculer 5% des zéros
        int zeroToKeep = (int) Math.ceil(zeroResponses.size() * 0.05);
        Collections.shuffle(zeroResponses); // Mélanger aléatoirement les zéros
        List<String> filteredZeroResponses = zeroResponses.subList(0, zeroToKeep);

        // Écrire les résultats dans un nouveau fichier
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            for (String response : nonZeroResponses) {
                writer.write(response);
                writer.newLine();
            }
            for (String response : filteredZeroResponses) {
                writer.write(response);
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture du fichier : " + e.getMessage());
            return;
        }

        // Remplacer l'ancien fichier par le fichier temporaire
        if (inputFile.delete() && tempFile.renameTo(inputFile)) {
            System.out.println("Fichier filtré mis à jour avec succès : " + inputFile.getAbsolutePath());
        } else {
            System.err.println("Erreur lors de la mise à jour du fichier filtré.");
        }
    }

}
