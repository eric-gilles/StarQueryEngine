package qengine.program;

import java.util.HashMap;

public class Dictionary {
    private final HashMap<String, Integer> dictionary;
    private int nextId;

    public Dictionary() {
        this.dictionary = new HashMap<>();
        this.nextId = 0;
    }

    public void add(String word) {
        if (!dictionary.containsKey(word)) {
            dictionary.put(word, nextId);
            nextId++;
        }
    }
    public Integer get(String key) {
        if (!dictionary.containsKey(key)) {
           return dictionary.get(key);
        }
        return null;
    }
}
