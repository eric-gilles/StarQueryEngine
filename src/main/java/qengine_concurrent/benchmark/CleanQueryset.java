package qengine_concurrent.benchmark;

import com.github.jsonldjava.shaded.com.google.common.collect.Iterators;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.parser.StarQuerySparQLParser;
import qengine_concurrent.storage.RDFHexaStore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CleanQueryset {

    private final String datasetPath;
    private String querysetDirPath;
    private String outputDirPath;

    public CleanQueryset(String datasetPath, String querysetDirPath, String outputDirPath) {
        this.datasetPath = datasetPath;
        this.querysetDirPath = querysetDirPath;
        this.outputDirPath = outputDirPath;
    }

    public void setQuerysetDirPath(String querysetDirPath) {
        this.querysetDirPath = querysetDirPath;
    }

    public void setOutputDirPath(String outputDirPath) {
        this.outputDirPath = outputDirPath;
    }

    public void supprimerDoublon() throws IOException {
        File dir = new File(querysetDirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("Le répertoire des fichiers queryset n'existe pas ou n'est pas un dossier.");
        }

        File[] querysetFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (querysetFiles == null) {
            throw new IOException("Aucun fichier queryset trouvé dans le répertoire spécifié.");
        }

        int totalQueries = 0;
        int totalDuplicates = 0;

        for (File file : querysetFiles) {
            Set<StarQuery> uniqueQueries = new HashSet<>();
            List<StarQuery> allQueries = new ArrayList<>();

            try (StarQuerySparQLParser parser = new StarQuerySparQLParser(file.getAbsolutePath())) {
                while (parser.hasNext()) {
                    StarQuery query = (StarQuery) parser.next();
                    allQueries.add(query);

                    if (!uniqueQueries.add(query)) {
                        System.out.println("Doublon détecté : " + query.getLabel());
                        totalDuplicates++;
                    }

                }
            }

            totalQueries += allQueries.size();
            writeQueriesToFile(uniqueQueries, file.getName());
        }

        System.out.println("Nombre total de requêtes : " + totalQueries);
        System.out.println("Nombre total de doublons supprimés : " + totalDuplicates);
        System.out.println("Nombre total de requêtes restantes : " + (totalQueries - totalDuplicates));

    }

    public void garderCinqPourcentDesRequeteZero() throws IOException {
        RDFHexaStore store = loadDataset(); // Charger les données du dataset
        File[] querysetFiles = new File(querysetDirPath).listFiles((d, name) -> name.endsWith(".queryset"));

        if (querysetFiles == null || querysetFiles.length == 0) {
            throw new FileNotFoundException("Aucun fichier queryset trouvé dans : " + querysetDirPath);
        }

        List<StarQuery> zeroResponseQueries = new ArrayList<>();
        Map<File, List<StarQuery>> fileToNonZeroQueries = new HashMap<>();

        // Collecter toutes les requêtes zéro et non zéro
        for (File file : querysetFiles) {
            List<StarQuery> nonZeroResponseQueries = new ArrayList<>();

            try (StarQuerySparQLParser parser = new StarQuerySparQLParser(file.getAbsolutePath())) {
                while (parser.hasNext()) {
                    StarQuery query = (StarQuery) parser.next();
                    long responseCount = Iterators.size(store.match(query)); // Compter les réponses

                    if (responseCount == 0) {
                        zeroResponseQueries.add(query);
                    } else {
                        nonZeroResponseQueries.add(query);
                    }
                }
            }

            fileToNonZeroQueries.put(file, nonZeroResponseQueries);
        }

        System.out.println("Total des requêtes zéro collectées : " + zeroResponseQueries.size());

        // Calculer 5% globalement
        int zeroToKeep = (int) Math.ceil(zeroResponseQueries.size() * 0.05);
        System.out.println("5% des requêtes zéro à conserver : " + zeroToKeep);

        // Sélectionner aléatoirement 5% des requêtes zéro
        Collections.shuffle(zeroResponseQueries);
        List<StarQuery> selectedZeroQueries = zeroResponseQueries.subList(0, Math.min(zeroToKeep, zeroResponseQueries.size()));

        // Réécrire les fichiers
        for (File file : querysetFiles) {
            List<StarQuery> finalQueries = new ArrayList<>(fileToNonZeroQueries.get(file));

            // Ajouter les requêtes zéro sélectionnées appartenant à ce fichier
            for (StarQuery query : selectedZeroQueries) {
                if (fileToNonZeroQueries.get(file).contains(query)) {
                    finalQueries.add(query);
                }
            }

            writeQueriesToFile(finalQueries, file.getName());
        }
    }


    private RDFHexaStore loadDataset() throws IOException {
        RDFHexaStore store = new RDFHexaStore();
        File datasetFile = new File(datasetPath);
        if (!datasetFile.exists()) {
            throw new FileNotFoundException("Fichier dataset introuvable : " + datasetPath);
        }

        try (FileReader reader = new FileReader(datasetFile)) {
            RDFAtomParser parser = new RDFAtomParser(reader, org.eclipse.rdf4j.rio.RDFFormat.NTRIPLES);
            while (parser.hasNext()) {
                store.add(parser.next());
            }
        }
        return store;
    }

    private void writeQueriesToFile(Collection<StarQuery> queries, String outputFileName) throws IOException {
        Path outputPath = Paths.get(outputDirPath, outputFileName);
        Files.createDirectories(outputPath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (StarQuery query : queries) {
                writer.write(query.getLabel());
                writer.newLine();
                writer.newLine();
            }
        }
    }



    public static void main(String[] args) {

        String datasetPath = "data/2M.nt";
        String querysetDirPath = "watdiv-mini-projet-partie-2/testsuite/queries";
        String outputDirPath = "watdiv-mini-projet-partie-2/testsuite/nodoublon_query";

        CleanQueryset cleaner = new CleanQueryset(datasetPath, querysetDirPath, outputDirPath);

        try {
            // Suppression des doublons
            System.out.println("Début du nettoyage des fichiers queryset...");
            cleaner.supprimerDoublon();
            System.out.println("Suppression des doublons terminée.");

            // Garder 5% des requêtes zéro
            System.out.println("Début de la suppression des 95% des requêtes zéro...");
            cleaner.setQuerysetDirPath("watdiv-mini-projet-partie-2/testsuite/nodoublon_query");
            cleaner.setOutputDirPath("watdiv-mini-projet-partie-2/testsuite/final_queryset");
            cleaner.garderCinqPourcentDesRequeteZero();

            // Nettoyage terminé
            System.out.println("Nettoyage terminé. Fichiers générés dans : " + outputDirPath);

        } catch (IOException e) {
            System.out.println("Une erreur est survenue lors du nettoyage.");
        }
    }

}

