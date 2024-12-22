package qengine_concurrent.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import qengine_concurrent.model.RDFAtom;
import org.junit.jupiter.api.Test;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.storage.RDFHexaStore;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests unitaires pour la classe {@link qengine_concurrent.storage.RDFHexaStore}.
 */
public class RDFHexaStoreTest {

    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Literal<String> SUBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("subject2");
    private static final Literal<String> PREDICATE_2 = SameObjectTermFactory.instance().createOrGetLiteral("predicate2");
    private static final Literal<String> OBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("object2");
    private static final Literal<String> OBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("object3");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");
    private static final Variable VAR_Z = SameObjectTermFactory.instance().createOrGetVariable("?z");
    private static final Literal<String> SUBJECT_5 = SameObjectTermFactory.instance().createOrGetLiteral("subject5");
    private static final Literal<String> PREDICATE_5 = SameObjectTermFactory.instance().createOrGetLiteral("predicate5");
    private static final Literal<String> OBJECT_5 = SameObjectTermFactory.instance().createOrGetLiteral("object5");

    @Test
    public void testAddAllRDFAtoms() {
        RDFHexaStore store = new RDFHexaStore();

        // Version stream
        // Ajouter plusieurs RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        Set<RDFAtom> rdfAtoms = Set.of(rdfAtom1, rdfAtom2);

        assertTrue(store.addAll(rdfAtoms.stream()), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");

        // Version collection
        store = new RDFHexaStore();
        assertTrue(store.addAll(rdfAtoms), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");
    }

    @Test
    public void testAddRDFAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        assertTrue(store.add(rdfAtom), "Le RDFAtom devrait être ajouté avec succès.");

        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom), "La base devrait contenir le RDFAtom ajouté.");
    }

    @Test
    public void testAddDuplicateAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        store.add(rdfAtom);
        long oldSize = store.size(); // Taille du HexaStore après l'ajout du premier atome
        assertFalse(store.add(rdfAtom), "Le RDFAtom ne devrait pas être ajouté s'il est déjà présent.");

        assertEquals(store.size(), oldSize, "La taille du HexaStore doit être la même après l'ajout d'un doublon.");
    }

    @Test
    public void testSize() {
        RDFHexaStore store = new RDFHexaStore();
        assertEquals(0, store.size(), "La taille d'un HexaStore vide doit être de 0.");

        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        store.add(rdfAtom1);
        assertEquals(1, store.size() , "Après l'ajout d'un atome, la taille du HexaStore doit être de 1.");

        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);
        store.add(rdfAtom2);
        assertEquals(2, store.size(), "Après l'ajout de deux atomes, la taille du HexaStore doit être de 2.");
    }

    @Test
    public void testMatchAtom() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1)); // RDFAtom(subject1, triple, object1)
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2)); // RDFAtom(subject2, triple, object2)
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3)); // RDFAtom(subject1, triple, object3)

        // CASE <x, y, z> where x or y or z doesn't exist in the dictionary store
        testMatchCaseNonExistingTermInStore(store);

        // CASE 0 | <x, y, z>
        testMatchAtomCase0(store);

        // CASE 1 | <x, y, ?z>
        testMatchAtomCase1(store);

        // CASE 2 | <x, ?y, z>
        testMatchAtomCase2(store);

        // CASE 3 | <?x, y, z>
        testMatchAtomCase3(store);

        // CASE 4 | <x, ?y, ?z>
        testMatchAtomCase4(store);

        // CASE 5 | <?x, ?y, z>
        testMatchAtomCase5(store);

        // CASE 6 | <?x, y, ?z>
        testMatchAtomCase6(store);

        // CASE 7 | <?x, ?y, ?z>
        testMatchAtomCase7(store);
    }

    private static void testMatchCaseNonExistingTermInStore(RDFHexaStore store) {

        RDFAtom matchingAtom = new RDFAtom(SUBJECT_5, PREDICATE_1, OBJECT_1); // RDFAtom(subject5, predicate1, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        assertTrue(matchedList.isEmpty(), "There should not be any substitution for a non existing RDFAtom");

        RDFAtom matchingAtom2 = new RDFAtom(SUBJECT_1, PREDICATE_5, OBJECT_1); // RDFAtom(subject1, predicate5, object1)
        Iterator<Substitution> matchedAtoms2 = store.match(matchingAtom2);
        List<Substitution> matchedList2 = new ArrayList<>();
        matchedAtoms2.forEachRemaining(matchedList2::add);

        assertTrue(matchedList2.isEmpty(), "There should not be any substitution for a non existing RDFAtom");

        RDFAtom matchingAtom3 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_5); // RDFAtom(subject1, predicate1, object5)
        Iterator<Substitution> matchedAtoms3 = store.match(matchingAtom3);
        List<Substitution> matchedList3 = new ArrayList<>();
        matchedAtoms3.forEachRemaining(matchedList3::add);

        assertTrue(matchedList3.isEmpty(), "There should not be any substitution for a non existing RDFAtom");


    }

    private static void testMatchAtomCase0(RDFHexaStore store) {
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1); // RDFAtom(subject1, predicate1, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);

        assertFalse(matchedAtoms.hasNext(), "There should not be any substitution for a non variable RDFAtom");
    }

    private static void testMatchAtomCase1(RDFHexaStore store) {
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X); // RDFAtom(subject1, predicate1, X)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, OBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, OBJECT_3);

        assertEquals(2, matchedList.size(), "There should be two matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
    }

    private static void testMatchAtomCase2(RDFHexaStore store) {
        RDFAtom matchingAtom2 = new RDFAtom(SUBJECT_1, VAR_X, OBJECT_1);
        Iterator<Substitution> matchedAtoms2 = store.match(matchingAtom2);
        List<Substitution> matchedList2 = new ArrayList<>();
        matchedAtoms2.forEachRemaining(matchedList2::add);

        Substitution firstResult2 = new SubstitutionImpl();
        firstResult2.add(VAR_X, PREDICATE_1);

        Substitution secondResult2 = new SubstitutionImpl();
        secondResult2.add(VAR_X, PREDICATE_2);

        assertEquals(1, matchedList2.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList2.contains(firstResult2), "Missing substitution: " + firstResult2);
        assertFalse(matchedList2.contains(secondResult2), "The atom doesn't exist: " + secondResult2);
    }

    private static void testMatchAtomCase3(RDFHexaStore store) {
        RDFAtom matchingAtom3 = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_3);
        Iterator<Substitution> matchedAtoms3 = store.match(matchingAtom3);
        List<Substitution> matchedList3 = new ArrayList<>();
        matchedAtoms3.forEachRemaining(matchedList3::add);

        Substitution firstResult3 = new SubstitutionImpl();
        firstResult3.add(VAR_X, SUBJECT_1);

        Substitution secondResult3 = new SubstitutionImpl();
        secondResult3.add(VAR_X, SUBJECT_2);

        assertEquals(1, matchedList3.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList3.contains(firstResult3), "Missing substitution: " + firstResult3);
        assertFalse(matchedList3.contains(secondResult3), "The atom doesn't exist: " + secondResult3);
    }

    private static void testMatchAtomCase4(RDFHexaStore store) {
        RDFAtom matchingAtom4 = new RDFAtom(SUBJECT_1, VAR_Y, VAR_X);
        Iterator<Substitution> matchedAtoms4 = store.match(matchingAtom4);
        List<Substitution> matchedList4 = new ArrayList<>();
        matchedAtoms4.forEachRemaining(matchedList4::add);

        Substitution firstResult4 = new SubstitutionImpl();
        firstResult4.add(VAR_Y, PREDICATE_1);
        firstResult4.add(VAR_X, OBJECT_1);

        Substitution secondResult4 = new SubstitutionImpl();
        secondResult4.add(VAR_Y, PREDICATE_2);
        secondResult4.add(VAR_X, OBJECT_2);

        assertEquals(2, matchedList4.size(), "There should be two matched RDFAtom");
        assertTrue(matchedList4.contains(firstResult4), "Missing substitution: " + firstResult4);
        assertFalse(matchedList4.contains(secondResult4), "The atom doesn't exist: " + secondResult4);
    }

    private static void testMatchAtomCase5(RDFHexaStore store) {
        RDFAtom matchingAtom5 = new RDFAtom(VAR_X, VAR_Y, OBJECT_2);
        Iterator<Substitution> matchedAtoms5 = store.match(matchingAtom5);
        List<Substitution> matchedList5 = new ArrayList<>();
        matchedAtoms5.forEachRemaining(matchedList5::add);

        Substitution firstResult5 = new SubstitutionImpl();
        firstResult5.add(VAR_X, SUBJECT_2);
        firstResult5.add(VAR_Y, PREDICATE_1);

        Substitution secondResult5 = new SubstitutionImpl();
        secondResult5.add(VAR_X, SUBJECT_2);
        secondResult5.add(VAR_Y, PREDICATE_2);

        assertEquals(1, matchedList5.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList5.contains(firstResult5), "Missing substitution: " + firstResult5);
        assertFalse(matchedList5.contains(secondResult5), "The atom doesn't exist: " + secondResult5);
    }

    private static void testMatchAtomCase6(RDFHexaStore store) {
        RDFAtom matchingAtom6 = new RDFAtom(VAR_X, PREDICATE_1, VAR_Y);
        Iterator<Substitution> matchedAtoms6 = store.match(matchingAtom6);
        List<Substitution> matchedList6 = new ArrayList<>();
        matchedAtoms6.forEachRemaining(matchedList6::add);

        Substitution firstResult6 = new SubstitutionImpl();
        firstResult6.add(VAR_X, SUBJECT_1);
        firstResult6.add(VAR_Y, OBJECT_1);

        Substitution secondResult6 = new SubstitutionImpl();
        secondResult6.add(VAR_X, SUBJECT_2);
        secondResult6.add(VAR_Y, OBJECT_3);

        assertEquals(3, matchedList6.size(), "There should be three matched RDFAtom");
        assertTrue(matchedList6.contains(firstResult6), "Missing substitution: " + firstResult6);
        assertFalse(matchedList6.contains(secondResult6), "Missing substitution: " + secondResult6);
    }

    private static void testMatchAtomCase7(RDFHexaStore store) {
        RDFAtom matchingAtom7 = new RDFAtom(VAR_X, VAR_Y, VAR_Z); // Toutes les composantes sont des variables
        Iterator<Substitution> matchedAtoms7 = store.match(matchingAtom7);
        List<Substitution> matchedList7 = new ArrayList<>();
        matchedAtoms7.forEachRemaining(matchedList7::add);

        // Création des substitutions attendues
        Substitution result7_1 = new SubstitutionImpl();
        result7_1.add(VAR_X, SUBJECT_1);
        result7_1.add(VAR_Y, PREDICATE_1);
        result7_1.add(VAR_Z, OBJECT_1);

        Substitution result7_2 = new SubstitutionImpl();
        result7_2.add(VAR_X, SUBJECT_2);
        result7_2.add(VAR_Y, PREDICATE_1);
        result7_2.add(VAR_Z, OBJECT_2);

        Substitution result7_3 = new SubstitutionImpl();
        result7_3.add(VAR_X, SUBJECT_1);
        result7_3.add(VAR_Y, PREDICATE_1);
        result7_3.add(VAR_Z, OBJECT_3);

        // Substitution non attendue
        Substitution result7_4 = new SubstitutionImpl();
        result7_4.add(VAR_X, SUBJECT_2);
        result7_4.add(VAR_Y, PREDICATE_2);
        result7_4.add(VAR_Z, OBJECT_2);

        // Vérification des résultats pour le cas 7
        assertEquals(3, matchedList7.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList7.contains(result7_1), "Missing substitution: " + result7_1);
        assertTrue(matchedList7.contains(result7_2), "Missing substitution: " + result7_2);
        assertTrue(matchedList7.contains(result7_3), "Missing substitution: " + result7_3);
        assertFalse(matchedList7.contains(result7_4), "The atom doesn't exist: " + result7_4);
    }

    @Test
    public void testMatchStarQuery() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1)); // RDFAtom(subject1, triple, object1)
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2)); // RDFAtom(subject2, triple, object2)
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3)); // RDFAtom(subject1, triple, object3)

        // Création de la requête en étoile
        List<RDFAtom> rdfAtoms = List.of(
                new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1),
                new RDFAtom(VAR_X, PREDICATE_1, OBJECT_3)
        );

        StarQuery starQuery = new StarQuery("StarQuery", rdfAtoms, List.of(VAR_X));

        // Exécution de la requête
        Iterator<Substitution> matchedAtoms = store.match(starQuery);
        Set<Substitution> matchedList = new HashSet<>();
        matchedAtoms.forEachRemaining(matchedList::add);

//        while (matchedAtoms.hasNext()) {
//            matchedList.add(matchedAtoms.next());
//        }

        // Création des substitutions attendues
        Substitution result1 = new SubstitutionImpl();
        result1.add(VAR_X, SUBJECT_1);

        Substitution result2 = new SubstitutionImpl();
        result2.add(VAR_X, SUBJECT_2);

        // Vérification des résultats
        assertEquals(1, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(result1), "Missing substitution: " + result1);
        assertFalse(matchedList.contains(result2), "Missing substitution: " + result2);
    }

    @Test
    public void testToString() {
        RDFHexaStore store = new RDFHexaStore();
        assertTrue(store.toString().startsWith("HexaStore"), "HexaStore toString should start with 'HexaStore'");
    }

    // Vos autres tests d'HexaStore ici

}
