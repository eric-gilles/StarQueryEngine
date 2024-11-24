package qengine.storage;

import fr.boreal.model.logicalElements.api.Term;

import java.util.HashMap;
import java.util.Map;

/**
 * La classe Dictionary représente un dictionnaire qui associe des termes à des index.
 * Elle permet de stocker des termes et de les retrouver par leur index, et vice versa.
 * Cette classe est utilisée dans le cadre de l'implémentation d'un HexaStore.
 */
public class Dictionary {
    private final HashMap<Term, Integer> dictionary;

    /**
     * Constructeur de la classe Dictionary.
     * Initialise un nouveau dictionnaire vide.
     */
    public Dictionary() {
        this.dictionary = new HashMap<>();
    }

    /**
     * Ajoute un terme au dictionnaire.
     *
     * @param term le terme à ajouter
     * @return true si le terme a été ajouté, false s'il existait déjà
     */
    public boolean add(Term term) {
        if (dictionary.containsKey(term)) {
            return false;
        }
        dictionary.put(term, dictionary.size());
        return true;
    }

    /**
     * Retourne l'index associé au terme donné.
     *
     * @param key le terme à rechercher
     * @return l'index du terme, ou null si le terme n'existe pas
     */
    public Integer get(Term key) {
        if (dictionary.containsKey(key)) {
           return dictionary.get(key);
        }
        return null;
    }

    /**
     * Retourne le terme associé à l'index donné.
     *
     * @param index l'index du terme à rechercher
     * @return le terme associé à l'index, ou null si l'index n'existe pas
     */
    public Term getKey(Integer index) {
        for (Map.Entry<Term, Integer> entry : dictionary.entrySet()) {
            if (entry.getValue().equals(index)) {
                return entry.getKey();
            }
        }
        return null;
    }

    /**
     * Ajoute un terme au dictionnaire s'il n'existe pas déjà et retourne son index.
     *
     * @param subject le terme à ajouter
     * @return l'index du terme dans le dictionnaire
     */
    public Integer addAndGet(Term subject) {
        return dictionary.computeIfAbsent(subject, k -> dictionary.size());
    }
}
