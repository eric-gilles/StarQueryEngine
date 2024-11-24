package qengine.storage;

import fr.boreal.model.logicalElements.api.Term;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


/**
 * Tests unitaires pour la classe {@link Dictionary}.
 */
class DictionnaryTest {

    @Test
    void addTermSuccessfully() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        assertTrue(dictionary.add(term), "The term should be added successfully.");
        assertEquals(0, dictionary.get(term), "The term should have index 0.");
    }

    @Test
    void addDuplicateTerm() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        dictionary.add(term);
        assertFalse(dictionary.add(term), "The term should not be added again.");
        assertEquals(0, dictionary.get(term), "The term should still have index 0.");
    }

    @Test
    void getExistingTerm() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        dictionary.add(term);
        assertEquals(0, dictionary.get(term), "The term should have index 0.");
    }

    @Test
    void getNonExistingTerm() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        assertNull(dictionary.get(term), "The term should not exist in the dictionary.");
    }

    @Test
    void getKeyByIndex() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        dictionary.add(term);
        assertEquals(term, dictionary.getKey(0), "The term should be retrieved by index 0.");
    }

    @Test
    void getKeyByNonExistingIndex() {
        Dictionary dictionary = new Dictionary();
        assertNull(dictionary.getKey(0), "There should be no term for index 0.");
    }

    @Test
    void addAndGetTerm() {
        Dictionary dictionary = new Dictionary();
        Term term = SameObjectTermFactory.instance().createOrGetLiteral("term1");
        assertEquals(0, dictionary.addAndGet(term), "The term should be added and have index 0.");
        assertEquals(0, dictionary.get(term), "The term should have index 0.");
    }

}
