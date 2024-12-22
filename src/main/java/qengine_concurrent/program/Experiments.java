package qengine_concurrent.program;

import fr.boreal.model.logicalElements.api.Atom;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.parser.RDFAtomParser;
import qengine_concurrent.storage.RDFHexaStore;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Experiments {

    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE = WORKING_DIR + "sample_data.nt";
    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";

    public static void main(String[] args) throws IOException {
        System.out.println("Parsing RDF Data");
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

        System.out.println("Adding elements to the HexaStore");
        RDFHexaStore hs = new RDFHexaStore();
        hs.addAll(rdfAtoms);

        System.out.println("Current state of the HexaStore:");
        System.out.println(hs);

        System.out.println("Resolved atoms using the dictionary:");
        for (Atom atom : hs.getAtoms()) {
            System.out.println(atom);
        }

        System.out.println("Total RDF atoms: " + hs.size());
    }

    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            int count = 0;
            while (rdfAtomParser.hasNext()) {
                RDFAtom atom = rdfAtomParser.next();
                rdfAtoms.add(atom);  // Stocker l'atome dans la collection
                System.out.println("RDF Atom #" + (++count) + ": " + atom);
            }
            System.out.println("Total RDF Atoms parsed: " + count);
        }
        return rdfAtoms;
    }

}
