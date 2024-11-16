package qengine.storage;

import fr.boreal.model.logicalElements.api.Term;

import java.util.HashMap;
import java.util.Map;

public class Dictionary {
    private final HashMap<Term, Integer> dictionary;
    private int nextId;

    public Dictionary() {
        this.dictionary = new HashMap<>();
        this.nextId = 0;
    }

    public boolean add(Term term) {
        if (dictionary.containsKey(term)) {
            return false;
        }
        dictionary.put(term, nextId);
        nextId++;
        return true;
    }

    public Integer get(Term key) {
        if (dictionary.containsKey(key)) {
           return dictionary.get(key);
        }
        return null;
    }

    public Term getKey(Integer index) {
        for (Map.Entry<Term, Integer> entry : dictionary.entrySet()) {
            if (entry.getValue().equals(index)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
