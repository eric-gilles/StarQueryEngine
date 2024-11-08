package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.jgrapht.alg.util.Pair;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.program.Dictionary;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {
    // Structure de donnée à revoir, dans le cas ou plusieurs sujet on plusieurs prédicat (utilisé hashmap) et pareil pour les relations prédicats/objets
    private HashMap<Integer, Pair<Integer, Integer>>SPO = new HashMap<>();
    private HashMap<Integer, Pair<Integer, Integer>>PSO = new HashMap<>();
    private HashMap<Integer, Pair<Integer, Integer>>OSP = new HashMap<>();
    private HashMap<Integer, Pair<Integer, Integer>>POS = new HashMap<>();
    private HashMap<Integer, Pair<Integer, Integer>>SOP = new HashMap<>();
    private HashMap<Integer, Pair<Integer, Integer>>OPS = new HashMap<>();
    private Dictionary dict = new Dictionary();
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


        SPO.put(sIndex, new Pair<>(pIndex, oIndex));
        PSO.put(pIndex, new Pair<>(sIndex, oIndex));
        OPS.put(oIndex, new Pair<>(pIndex, sIndex));
        OSP.put(oIndex, new Pair<>(sIndex, pIndex));
        POS.put(pIndex, new Pair<>(oIndex, sIndex));
        SOP.put(sIndex, new Pair<>(oIndex, pIndex));
        return true;
    }

    @Override
    public long size() {
        throw new NotImplementedException();
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
