package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.program.Dictionary;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>SPO = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>PSO = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>OSP = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>POS = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>SOP = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>>OPS = new HashMap<>();
    private final Dictionary dict = new Dictionary();
    @Override
    public boolean add(RDFAtom atom) {
        String s = atom.getTripleSubject().label();
        String p = atom.getTriplePredicate().label();
        String o = atom.getTripleObject().label();

        dict.add(s);
        dict.add(p);
        dict.add(o);

        Integer sIndex = dict.get(s);
        Integer pIndex = dict.get(p);
        Integer oIndex = dict.get(o);

        // SPO
        SPO.computeIfAbsent(sIndex, k -> new HashMap<>())
                .computeIfAbsent(pIndex, k -> new HashSet<>())
                .add(oIndex);

        // PSO
        PSO.computeIfAbsent(pIndex, k -> new HashMap<>())
                .computeIfAbsent(sIndex, k -> new HashSet<>())
                .add(oIndex);

        // OSP
        OSP.computeIfAbsent(oIndex, k -> new HashMap<>())
                .computeIfAbsent(sIndex, k -> new HashSet<>())
                .add(pIndex);

        // POS
        POS.computeIfAbsent(pIndex, k -> new HashMap<>())
                .computeIfAbsent(oIndex, k -> new HashSet<>())
                .add(sIndex);

        // SOP
        SOP.computeIfAbsent(sIndex, k -> new HashMap<>())
                .computeIfAbsent(oIndex, k -> new HashSet<>())
                .add(pIndex);

        // OPS
        OPS.computeIfAbsent(oIndex, k -> new HashMap<>())
                .computeIfAbsent(pIndex, k -> new HashSet<>())
                .add(sIndex);


        return true;
    }

    @Override
    public long size() {
        long sum = 0L;

        for (HashMap<Integer, Set<Integer>> predicates : SOP.values()) {
            sum += 32L * predicates.size(); // Estimation de la taille des entrées de HashMap (32 octets par entrée)
            for (Set<Integer> objects : predicates.values()) {
                // Estimation de la surcharge pour un Set (environ 64 octets pour la structure interne)
                sum += 64L;
                // Taille de chaque Integer dans le Set (4 octets)
                sum += (long) objects.size() * Integer.SIZE / 8;
            }
        }
        return sum;
    }


    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<Atom> getAtoms() {
        throw new NotImplementedException();
    }
}
