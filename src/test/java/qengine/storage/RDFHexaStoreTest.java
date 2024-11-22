package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Disabled;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.storage.RDFHexaStore;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests unitaires pour la classe {@link RDFHexaStore}.
 */
class RDFHexaStoreTest {
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


    @Test
    void testAddAllRDFAtoms() {
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
    void testAddRDFAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        assertTrue(store.add(rdfAtom), "The RDFAtom should be added successfully.");

        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom), "The store should contain the added RDFAtom.");
    }

    @Test
    void testAddDuplicateAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        assertTrue(store.add(rdfAtom), "The RDFAtom should be added successfully.");
        assertFalse(store.add(rdfAtom), "The RDFAtom should not be added again.");

        Collection<Atom> atoms = store.getAtoms();
        assertEquals(1, atoms.size(), "There should be only one RDFAtom in the store.");
        assertTrue(atoms.contains(rdfAtom), "The store should contain the added RDFAtom.");
    }

    @Test
    void testSize() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        assertEquals(0, store.size(), "The store should be empty.");

        store.add(rdfAtom1);
        assertEquals(1, store.size(), "The store should contain one RDFAtom.");

        store.add(rdfAtom2);
        assertEquals(2, store.size(), "The store should contain two RDFAtoms.");
    }

    @Test
    void testMatchAtom() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

        // Test case 1 : Constante, Constante, Variable
        testMatchAtom1(store);// Test case 1 : Constante, Constante, Variable

        testMatchAtom2(store); // Test case 2 : Constante, Variable, Constante

        testMatchAtom3(store); // Test case 3 : Constante, Variable, Variable

        testMatchAtom4(store); // Test case 4 : Variable, Constante, Constante

        testMatchAtom5(store); // Test case 5 : Variable, Constante, Variable

        testMatchAtom6(store); // Test case 6 : Variable, Variable, Constante

        testMatchAtom7(store); // Test case 7 : Variable, Variable, Variable
    }



    void testMatchAtom1(RDFHexaStore store){ // Test case 1 : Constante, Constante, Variable
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X); // RDFAtom(subject1, predicate1, X)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, OBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, OBJECT_3);
        Substitution thirdResult = new SubstitutionImpl();
        thirdResult.add(VAR_X, OBJECT_2);
        assertEquals(3, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution: " + secondResult);
    }

    void testMatchAtom2(RDFHexaStore store) { // Test case 2 : Constante, Variable, Constante
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, VAR_X, OBJECT_1); // RDFAtom(subject1, X, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, PREDICATE_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, PREDICATE_2);
        assertEquals(1, matchedList.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertFalse(matchedList.contains(secondResult), "There should be no substitution: " + secondResult);
    }

    void testMatchAtom3(RDFHexaStore store) { // Test case 3 : Constante, Variable, Variable
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, VAR_X, VAR_Y); // RDFAtom(subject1, X, Y)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, PREDICATE_1);
        firstResult.add(VAR_Y, OBJECT_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, PREDICATE_2);
        secondResult.add(VAR_Y, OBJECT_2);

        assertEquals(3, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertFalse(matchedList.contains(secondResult), "There should be no substitution: " + secondResult);
    }

    void testMatchAtom4(RDFHexaStore store) { // Test case 4 : Variable, Constante, Constante
        RDFAtom matchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1); // RDFAtom(X, predicate1, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_2);

        assertEquals(1, matchedList.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertFalse(matchedList.contains(secondResult), "There should be no substitution: " + secondResult);
    }

    void testMatchAtom5(RDFHexaStore store) { // Test case 5 : Variable, Constante, Variable
        RDFAtom matchingAtom = new RDFAtom(VAR_X, PREDICATE_1, VAR_Y); // RDFAtom(X, predicate1, Y)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, OBJECT_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_1);
        secondResult.add(VAR_Y, OBJECT_3);

        Substitution thirdResult = new SubstitutionImpl();
        thirdResult.add(VAR_X, SUBJECT_1);
        thirdResult.add(VAR_Y, OBJECT_2);

        Substitution fourthResult = new SubstitutionImpl();
        fourthResult.add(VAR_X, SUBJECT_2);
        fourthResult.add(VAR_Y, OBJECT_2);

        Substitution fifthResult = new SubstitutionImpl();
        fifthResult.add(VAR_X, SUBJECT_2);
        fifthResult.add(VAR_Y, OBJECT_3);

        assertEquals(5, matchedList.size(), "There should be five matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(fourthResult), "Missing substitution: " + fourthResult);
        assertTrue(matchedList.contains(fifthResult), "Missing substitution: " + fifthResult);
    }

    void testMatchAtom6(RDFHexaStore store) { // Test case 6 : Variable, Variable, Constante
        RDFAtom matchingAtom = new RDFAtom(VAR_X, VAR_Y, OBJECT_1); // RDFAtom(X, Y, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_2);
        firstResult.add(VAR_Y, PREDICATE_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_2);
        secondResult.add(VAR_Y, PREDICATE_2);

        assertEquals(1, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertFalse(matchedList.contains(secondResult), "There should be no substitution: " + secondResult);
    }

    void testMatchAtom7(RDFHexaStore store) { // Test case 7 : Variable, Variable, Variable
        RDFAtom matchingAtom = new RDFAtom(VAR_X, VAR_Y, VAR_Z); // RDFAtom(X, Y, Z)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, PREDICATE_1);
        firstResult.add(VAR_Z, OBJECT_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_2);
        secondResult.add(VAR_Y, PREDICATE_1);
        secondResult.add(VAR_Z, OBJECT_2);

        Substitution thirdResult = new SubstitutionImpl();
        thirdResult.add(VAR_X, SUBJECT_1);
        thirdResult.add(VAR_Y, PREDICATE_1);
        thirdResult.add(VAR_Z, OBJECT_3);

        Substitution fourthResult = new SubstitutionImpl();
        fourthResult.add(VAR_X, SUBJECT_2);
        fourthResult.add(VAR_Y, PREDICATE_2);
        fourthResult.add(VAR_Z, OBJECT_2);

        assertEquals(3, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution: " + thirdResult);
        assertFalse(matchedList.contains(fourthResult), "There should be no substitution: " + fourthResult);
    }

    @Test
    public void testMatchStarQuery() {
        RDFHexaStore store = new RDFHexaStore();
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_2, VAR_Y);
        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        vars.add(VAR_Y);
        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        Iterator<Substitution> matchedAtoms = store.match(q);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, OBJECT_3);
        assertEquals(1, matchedList.size(), "There should be two matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
    }

    // Vos autres tests d'HexaStore ici
}
