package qengine.program;

import java.util.HashMap;

public class Dictionary {
    private final HashMap<Integer, String> dictionary;
    private int nextId;

    public Dictionary() {
        this.dictionary = new HashMap<>();
        this.nextId = 0;
    }

    public void add(String word) {
        if (!dictionary.containsValue(word)) {
            dictionary.put(nextId, word);
            nextId++;
        }
    }
}
