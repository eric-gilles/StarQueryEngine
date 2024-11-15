package qengine.program;

import java.util.HashMap;
import java.util.Map;

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
    public String getKey(Integer index) {
        for (Map.Entry<String, Integer> entry : dictionary.entrySet()) {
            if (entry.getValue().equals(index)) {
                return entry.getKey();
            }
        }
        return null;
    }

}
