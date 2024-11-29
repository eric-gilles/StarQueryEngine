package qengine.storage;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import org.junit.jupiter.api.Test;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;

import java.io.FileReader;
import java.io.IOException;
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

    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE_SMALL = WORKING_DIR + "sample_data.nt";
    private static final String SAMPLE_DATA_FILE_BIG = WORKING_DIR + "100K.nt";

    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";
    private static final String SAMPLE_QUERY_FILE_ALL = WORKING_DIR + "STAR_ALL_workload.queryset";


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
    void testMatchNonExistingAtom() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        assertFalse(matchedAtoms.hasNext(), "There should be no matched RDFAtoms.");
        assertTrue(matchedList.isEmpty(), "The list of matched RDFAtoms should be empty.");
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


        testMatchAtom0(store);// Test case 0 : No Variables

        testMatchAtom1(store);// Test case 1 : Constante, Constante, Variable

        testMatchAtom2(store); // Test case 2 : Constante, Variable, Constante

        testMatchAtom3(store); // Test case 3 : Constante, Variable, Variable

        testMatchAtom4(store); // Test case 4 : Variable, Constante, Constante

        testMatchAtom5(store); // Test case 5 : Variable, Constante, Variable

        testMatchAtom6(store); // Test case 6 : Variable, Variable, Constante

        testMatchAtom7(store); // Test case 7 : Variable, Variable, Variable
    }

    void testMatchAtom0(RDFHexaStore store) {
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);
        Substitution firstResult = new SubstitutionImpl();

        assertEquals(1, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
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


        assertEquals(1, matchedList.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
    }

    void testMatchAtom3(RDFHexaStore store) { // Test case 3 : Constante, Variable, Variable
        RDFAtom matchingAtom = new RDFAtom(SUBJECT_1, VAR_X, VAR_Y); // RDFAtom(subject1, X, Y)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        Substitution secondResult = new SubstitutionImpl();
        Substitution thirdResult = new SubstitutionImpl();
        Substitution fourthResult = new SubstitutionImpl();

        firstResult.add(VAR_X, PREDICATE_1);
        firstResult.add(VAR_Y, OBJECT_1);

        secondResult.add(VAR_X, PREDICATE_1);
        secondResult.add(VAR_Y, OBJECT_3);

        thirdResult.add(VAR_X, PREDICATE_2);
        thirdResult.add(VAR_Y, OBJECT_3);

        fourthResult.add(VAR_X, PREDICATE_1);
        fourthResult.add(VAR_Y, OBJECT_2);

        assertEquals(4, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution : " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution : " + thirdResult);
        assertTrue(matchedList.contains(fourthResult), "Missing substitution : " + fourthResult);
    }

    void testMatchAtom4(RDFHexaStore store) { // Test case 4 : Variable, Constante, Constante
        RDFAtom matchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1); // RDFAtom(X, predicate1, object1)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);

        assertEquals(1, matchedList.size(), "There should be one matched RDFAtom");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
    }

    void testMatchAtom5(RDFHexaStore store) { // Test case 5 : Variable, Constante, Variable
        RDFAtom matchingAtom = new RDFAtom(VAR_X, PREDICATE_1, VAR_Y); // RDFAtom(X, predicate1, Y)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);

        Substitution firstResult = new SubstitutionImpl();
        Substitution secondResult = new SubstitutionImpl();
        Substitution thirdResult = new SubstitutionImpl();
        Substitution fourthResult = new SubstitutionImpl();
        Substitution fifthResult = new SubstitutionImpl();


        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, OBJECT_1);

        secondResult.add(VAR_X, SUBJECT_1);
        secondResult.add(VAR_Y, OBJECT_3);

        thirdResult.add(VAR_X, SUBJECT_1);
        thirdResult.add(VAR_Y, OBJECT_2);

        fourthResult.add(VAR_X, SUBJECT_2);
        fourthResult.add(VAR_Y, OBJECT_2);

        fifthResult.add(VAR_X, SUBJECT_2);
        fifthResult.add(VAR_Y, OBJECT_3);

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

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
        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, PREDICATE_1);

        assertEquals(1, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
    }

    void testMatchAtom7(RDFHexaStore store) { // Test case 7 : Variable, Variable, Variable
        RDFAtom matchingAtom = new RDFAtom(VAR_X, VAR_Y, VAR_Z); // RDFAtom(X, Y, Z)
        Iterator<Substitution> matchedAtoms = store.match(matchingAtom);
        List<Substitution> matchedList = new ArrayList<>();
        matchedAtoms.forEachRemaining(matchedList::add);
        Substitution firstResult = new SubstitutionImpl();
        Substitution secondResult = new SubstitutionImpl();
        Substitution thirdResult = new SubstitutionImpl();
        Substitution fourthResult = new SubstitutionImpl();
        Substitution fifthResult = new SubstitutionImpl();
        Substitution sixthResult = new SubstitutionImpl();


        firstResult.add(VAR_X,SUBJECT_1);
        firstResult.add(VAR_Y,PREDICATE_1);
        firstResult.add(VAR_Z,OBJECT_1);

        secondResult.add(VAR_X,SUBJECT_2);
        secondResult.add(VAR_Y,PREDICATE_1);
        secondResult.add(VAR_Z,OBJECT_2);

        thirdResult.add(VAR_X,SUBJECT_1);
        thirdResult.add(VAR_Y,PREDICATE_1);
        thirdResult.add(VAR_Z,OBJECT_3);

        fourthResult.add(VAR_X,SUBJECT_1);
        fourthResult.add(VAR_Y,PREDICATE_2);
        fourthResult.add(VAR_Z,OBJECT_3);

        fifthResult.add(VAR_X,SUBJECT_2);
        fifthResult.add(VAR_Y,PREDICATE_1);
        fifthResult.add(VAR_Z,OBJECT_3);


        sixthResult.add(VAR_X,SUBJECT_1);
        sixthResult.add(VAR_Y,PREDICATE_1);
        sixthResult.add(VAR_Z,OBJECT_2);

        assertEquals(6, matchedList.size(), "There should be three matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution: " + thirdResult);
        assertTrue(matchedList.contains(fourthResult), "Missing substitution: " + fourthResult);
        assertTrue(matchedList.contains(fifthResult), "Missing substitution : " + fifthResult);
        assertTrue(matchedList.contains(sixthResult), "Missing substitution : " + sixthResult);
    }

    private List<Substitution> executeStarQuery(StarQuery starQuery, FactBase factBase){
        FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery(); // Conversion en FOQuery
        FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance(); // Créer un évaluateur
        Iterator<Substitution> queryResults = evaluator.evaluate(foQuery, factBase); // Évaluer la requête
        List<Substitution> matchedResult = new ArrayList<>();
        queryResults.forEachRemaining(matchedResult::add);
        return matchedResult;
    }


    private List<Substitution> executeStarQueryHexaStore(StarQuery starQuery, RDFHexaStore store){
        Iterator<Substitution> matchedSubstitutions =  store.match(starQuery);
        List<Substitution> matchedResult = new ArrayList<>();
        matchedSubstitutions.forEachRemaining(matchedResult::add);
        return matchedResult;
    }

    @Test
    public void testMatchStarQuery() {

        RDFHexaStore store = new RDFHexaStore();
        List<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        rdfAtoms.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_2));
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));
        rdfAtoms.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));
        FactBase factBase = new SimpleInMemoryGraphStore();

        // Stocker chaque RDFAtom dans le store
        for (RDFAtom atom : rdfAtoms) {
            factBase.add(atom);
            store.add(atom);
        }
        testMatchStarQueryMultipleVars1(store, factBase);
        testMatchStarQueryMultipleVars2(store, factBase);
        testMatchStarQuery1(store, factBase);
        testMatchStarQuery2(store, factBase);
        testMatchStarQuery3(store, factBase);
        testMatchStarQuery4(store, factBase);

    }
    @Test
    public void testMultipleValues(){
        RDFHexaStore store = new RDFHexaStore();
        FactBase factBase = new SimpleInMemoryGraphStore();

        List<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        rdfAtoms.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_1));
        rdfAtoms.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        rdfAtoms.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        for (RDFAtom rdfAtom: rdfAtoms){
            factBase.add(rdfAtom);
            store.add(rdfAtom);
        }


        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_3);


        ArrayList<RDFAtom> selectedAtoms = new ArrayList<>();
        selectedAtoms.add(secondMatchingAtom);
        selectedAtoms.add(firstMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        StarQuery q = new StarQuery("My star query", selectedAtoms, vars);
        List<Substitution> res = executeStarQueryHexaStore(q, store);
        List<Substitution> integraal = executeStarQuery(q, factBase);
        Substitution firstRes = new SubstitutionImpl();
        firstRes.add(VAR_X, SUBJECT_1);
        Substitution secondRes = new SubstitutionImpl();
        secondRes.add(VAR_X, SUBJECT_2);
        assertEquals(2, res.size(), "There should be two matched RDFAtoms");
        assertEquals(integraal.size(), res.size(), "Intregraal and Hexstore should send the same result");
        assertTrue(res.containsAll(integraal), "Integraal doesn't have the same result:"+ integraal);
        assertTrue(integraal.containsAll(res), "Integraal doesn't have the same result:"+ integraal);
    }

    public void testMatchStarQueryMultipleVars1(RDFHexaStore store, FactBase factBase){

        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, VAR_Y);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_3);
        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        vars.add(VAR_Y);

        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);


        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        firstResult.add(VAR_Y, OBJECT_1);

        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_1);
        secondResult.add(VAR_Y, OBJECT_2);
        Substitution thirdResult = new SubstitutionImpl();
        thirdResult.add(VAR_X, SUBJECT_1);
        thirdResult.add(VAR_Y, OBJECT_3);

        assertEquals(3, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing substitution: " + secondResult);
        assertTrue(matchedList.contains(thirdResult), "Missing substitution: " + thirdResult);
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);    }


    public void testMatchStarQueryMultipleVars2(RDFHexaStore store, FactBase factBase){
        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, VAR_Y);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, VAR_Z, OBJECT_3);
        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        vars.add(VAR_Y);
        vars.add(VAR_Z);

        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);

        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);

        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        Substitution one = new SubstitutionImpl();
        Substitution two = new SubstitutionImpl();
        Substitution three = new SubstitutionImpl();
        Substitution four = new SubstitutionImpl();
        Substitution five = new SubstitutionImpl();
        Substitution six = new SubstitutionImpl();
        Substitution seven = new SubstitutionImpl();
        Substitution eight = new SubstitutionImpl();

        one.add(VAR_X, SUBJECT_1);
        one.add(VAR_Z, PREDICATE_1);
        one.add(VAR_Y, OBJECT_1);


        two.add(VAR_X, SUBJECT_1 );
        two.add(VAR_Z, PREDICATE_2);
        two.add(VAR_Y, OBJECT_1);

        three.add(VAR_X, SUBJECT_2 );
        three.add(VAR_Z, PREDICATE_1);
        three.add(VAR_Y, OBJECT_2);

        four.add(VAR_X, SUBJECT_1);
        four.add(VAR_Z, PREDICATE_1);
        four.add(VAR_Y, OBJECT_3);


        five.add(VAR_X, SUBJECT_1);
        five.add(VAR_Z, PREDICATE_2);
        five.add(VAR_Y, OBJECT_3);

        six.add(VAR_X, SUBJECT_1);
        six.add(VAR_Z, PREDICATE_1);
        six.add(VAR_Y, OBJECT_2);

        seven.add(VAR_X, SUBJECT_1);
        seven.add(VAR_Z, PREDICATE_2);
        seven.add(VAR_Y, OBJECT_2);

        eight.add(VAR_X, SUBJECT_2);
        eight.add(VAR_Z, PREDICATE_1);
        eight.add(VAR_Y, OBJECT_3);


        assertEquals(8, matchedList.size(), "There should be 8 matched RDFAtoms");
        assertTrue(matchedList.contains(one), "Missing substitution: " + one);
        assertTrue(matchedList.contains(two), "Missing substitution: " + two);
        assertTrue(matchedList.contains(three), "Missing substitution: " + three);
        assertTrue(matchedList.contains(four), "Missing substitution: " + four);
        assertTrue(matchedList.contains(five), "Missing substitution: " + five);
        assertTrue(matchedList.contains(six), "Missing substitution: " + six);
        assertTrue(matchedList.contains(seven), "Missing substitution: " + seven);
        assertTrue(matchedList.contains(eight), "Missing substitution: " + eight);
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);
    }

    public void testMatchStarQuery1(RDFHexaStore store, FactBase factBase){
        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_3);
        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);

        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        assertEquals(1, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing substitution: " + firstResult);
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);    }

    public void testMatchStarQuery2(RDFHexaStore store, FactBase factBase){
        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_1);
        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);

        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        assertEquals(0, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);

    }

    public void testMatchStarQuery3(RDFHexaStore store, FactBase factBase){
        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_2);

        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);

        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        assertEquals(1, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing element: " + firstResult);
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);
    }

    public void testMatchStarQuery4(RDFHexaStore store, FactBase factBase){
        RDFAtom firstMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_2);
        RDFAtom secondMatchingAtom = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_3);

        ArrayList<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(firstMatchingAtom);
        rdfAtoms.add(secondMatchingAtom);
        ArrayList<Variable> vars = new ArrayList<>();
        vars.add(VAR_X);
        StarQuery q = new StarQuery("My star query", rdfAtoms, vars);
        List<Substitution> matchedList  = executeStarQueryHexaStore(q, store);

        List<Substitution> resultIntegral = executeStarQuery(q, factBase);

        Substitution firstResult = new SubstitutionImpl();
        firstResult.add(VAR_X, SUBJECT_1);
        Substitution secondResult = new SubstitutionImpl();
        secondResult.add(VAR_X, SUBJECT_2);
        assertEquals(2, matchedList.size(), "There should be one matched RDFAtoms");
        assertTrue(matchedList.contains(firstResult), "Missing element: " + firstResult);
        assertTrue(matchedList.contains(secondResult), "Missing element: " + secondResult);
        assertTrue(matchedList.containsAll(resultIntegral), "Integraal doesn't have the same result:"+ resultIntegral);
        assertTrue(resultIntegral.containsAll(matchedList), "Integraal doesn't have the same result:"+ resultIntegral);    }

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
            while (rdfAtomParser.hasNext()) {
                RDFAtom atom = rdfAtomParser.next();
                rdfAtoms.add(atom);  // Stocker l'atome dans la collection
            }
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

            while (queryParser.hasNext()) {
                Query query = queryParser.next();
                if (query instanceof StarQuery starQuery) {
                    starQueries.add(starQuery);  // Stocker la requête dans la collection
                    starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
                } else {
                    System.err.println("Requête inconnue ignorée.");
                }
            }
            System.out.println("Total Queries parsed: " + starQueries.size());
        }
        return starQueries;
    }
    @Test
    public void testCorrectionCompletude() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_SMALL);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);
        }
        evaluationOfQueries(starQueries, factBase, store);

        rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_BIG);
        starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE_ALL);
        factBase = new SimpleInMemoryGraphStore();
        store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        evaluationOfQueries(starQueries, factBase, store);

    }
    public void evaluationOfQueries(List<StarQuery> queries, FactBase factBase, RDFHexaStore store){
        for (StarQuery starQuery : queries) {
            // Conversion en FOQuery
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


    @Test
    public void testSpeedOnBigDataset() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_BIG);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE_ALL);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        speedTest(starQueries, store, factBase);
    }
    @Test
    public void testSpeedOnSmallDataset() throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE_SMALL);
        List<StarQuery> starQueries = parseSparQLQueries(SAMPLE_QUERY_FILE);
        FactBase factBase = new SimpleInMemoryGraphStore();
        RDFHexaStore store = new RDFHexaStore();
        for (RDFAtom atom : rdfAtoms) {
            store.add(atom);
            factBase.add(atom);  // Stocker chaque RDFAtom dans le store
        }
        speedTest(starQueries, store, factBase);
    }

    private void speedTest(List<StarQuery> queries, RDFHexaStore store, FactBase factBase) {
        long startTimeIntegraal = System.currentTimeMillis();

        for (StarQuery starQuery : queries) {
            // Conversion en FOQuery
            FOQuery<FOFormulaConjunction> foQuery = starQuery.asFOQuery();
            FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
            evaluator.evaluate(foQuery, factBase);
        }
        long endTimeIntegraal = System.currentTimeMillis();
        long durationIntegraal = endTimeIntegraal - startTimeIntegraal;

        long startTimeHexastore = System.currentTimeMillis();
        for(StarQuery starQuery: queries){
            // Mesurer le temps de HexaStore
            store.match(starQuery);
        }
        long endTimeHexastore = System.currentTimeMillis();
        long durationHexastore = endTimeHexastore - startTimeHexastore;


        // Calculer les moyennes
        System.out.println("Average execution time of Integraal: " + (double) durationIntegraal / queries.size() + " ms");
        System.out.println("Average execution time of our HexaStore: " + (double) durationHexastore / queries.size() + " ms");
    }
}
