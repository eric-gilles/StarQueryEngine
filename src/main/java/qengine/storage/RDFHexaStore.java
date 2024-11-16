package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {
    // Indexes
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> spo = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> pso = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> osp = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> pos = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> sop = new HashMap<>();
    private final HashMap<Integer, HashMap<Integer, Set<Integer>>> ops = new HashMap<>();

    private final Dictionary dict = new Dictionary();
    private int size = 0;

    @Override
    public boolean add(RDFAtom atom) {
        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        dict.add(subject);
        dict.add(predicate);
        dict.add(object);

        Integer sIndex = dict.get(subject);
        Integer pIndex = dict.get(predicate);
        Integer oIndex = dict.get(object);

        if (spo.containsKey(sIndex) && spo.get(sIndex).containsKey(pIndex) && spo.get(sIndex).get(pIndex).contains(oIndex)) {
            return false;
        }

        size++;

        addToAllIndex(sIndex, pIndex, oIndex);

        return true;
    }

    private void addToAllIndex(Integer sIndex, Integer pIndex, Integer oIndex) {
        addToIndex(spo, sIndex, pIndex, oIndex);
        addToIndex(pso, pIndex, sIndex, oIndex);
        addToIndex(ops, oIndex, pIndex, sIndex);
        addToIndex(pos, pIndex, oIndex, sIndex);
        addToIndex(sop, sIndex, oIndex, pIndex);
        addToIndex(osp, oIndex, sIndex, pIndex);
    }

    private void addToIndex(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex, Integer thirdIndex) {
        hashMap.computeIfAbsent(firstIndex, k -> new HashMap<>())
                .computeIfAbsent(secondIndex, k -> new HashSet<>())
                .add(thirdIndex);
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        List<Substitution> substitutions = new ArrayList<>();

        Integer sIndex = dict.get(subject);
        Integer pIndex = dict.get(predicate);
        Integer oIndex = dict.get(object);

        AtomMatchType atomMatchType = determineTermType(subject, predicate, object);
        switch (atomMatchType){
            case CONST_CONST_CONST -> { // Subject: Constant, Predicate: Constant, Object: Constant
                if (spo.containsKey(sIndex) && spo.get(sIndex).containsKey(pIndex) && spo.get(sIndex).get(pIndex).contains(oIndex)) {
                    substitutions.add(new SubstitutionImpl());
                }
            }
            case CONST_CONST_VAR -> { // Subject: Constant, Predicate: Constant, Object: Variable
                if (spo.containsKey(sIndex) && spo.get(sIndex).containsKey(pIndex)) {
                    for (Integer o : spo.get(sIndex).get(pIndex)) {
                        addSubstitution(substitutions, object.label(), dict.getKey(o));
                    }
                }
            }
            case CONST_VAR_CONST -> { // Subject: Constant, Predicate: Variable, Object: Constant
                if (spo.containsKey(sIndex)) {
                    for (Map.Entry<Integer, Set<Integer>> entry : spo.get(sIndex).entrySet()) {
                        Integer p = entry.getKey();
                        if (entry.getValue().contains(oIndex)) {
                            addSubstitution(substitutions, predicate.label(), dict.getKey(p));
                        }
                    }
                }
            }
            case CONST_VAR_VAR -> { // Subject: Constant, Predicate: Variable, Object: Variable
                if (spo.containsKey(sIndex)) {
                    for (Map.Entry<Integer, HashMap<Integer, Set<Integer>>> entry : spo.entrySet()) {
                        Integer s = entry.getKey();
                        if (sIndex.equals(s)) {
                            for (Map.Entry<Integer, Set<Integer>> entry1 : entry.getValue().entrySet()) {
                                Integer p = entry1.getKey();
                                for (Integer o : entry1.getValue()) {
                                    addSubstitution(substitutions, predicate.label(), dict.getKey(p));
                                    addSubstitution(substitutions, object.label(), dict.getKey(o));
                                }
                            }
                        }
                    }
                }
            }
            case VAR_CONST_CONST -> { // Subject: Variable, Predicate: Constant, Object: Constant
                if (pso.containsKey(pIndex) && pso.get(pIndex).containsKey(oIndex)) {
                    for (Integer s : pso.get(pIndex).get(oIndex)) {
                        addSubstitution(substitutions, subject.label(), dict.getKey(s));
                    }
                }
            }
            case VAR_CONST_VAR -> // Subject: Variable, Predicate: Constant, Object: Variable
                getSubstitutionsFor2Var(pso, sIndex, predicate, object, substitutions);

            case VAR_VAR_CONST ->  // Subject: Variable, Predicate: Variable, Object: Constant
                getSubstitutionsFor2Var(ops, oIndex, subject, predicate, substitutions);

            case VAR_VAR_VAR -> { // Subject: Variable, Predicate: Variable, Object: Variable
                for (Map.Entry<Integer, HashMap<Integer, Set<Integer>>> entry : spo.entrySet()) {
                    Integer s = entry.getKey();
                    for (Map.Entry<Integer, Set<Integer>> entry1 : entry.getValue().entrySet()) {
                        Integer p = entry1.getKey();
                        for (Integer o : entry1.getValue()) {
                            addSubstitution(substitutions, subject.label(), dict.getKey(s));
                            addSubstitution(substitutions, predicate.label(), dict.getKey(p));
                            addSubstitution(substitutions, object.label(), dict.getKey(o));
                        }
                    }
                }
            }
            case null -> {}
            default -> {}
        }
        return substitutions.iterator();
    }

    private void getSubstitutionsFor2Var(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex,
                                         Term firstTerm, Term secondTerm, List<Substitution> substitutions) {
        if (hashMap.containsKey(firstIndex)) {
            for (Map.Entry<Integer, Set<Integer>> entry : ops.get(firstIndex).entrySet()) {
                Integer p = entry.getKey();
                for (Integer s : entry.getValue()) {
                    addSubstitution(substitutions, firstTerm.label(), dict.getKey(s));
                    addSubstitution(substitutions, secondTerm.label(), dict.getKey(p));
                }
            }
        }

    }

    private void addSubstitution(List<Substitution> substitutions, String termLabel, Term dictKey) {
        Substitution substitution = new SubstitutionImpl();
        substitution.add(SameObjectTermFactory.instance().createOrGetVariable(termLabel), dictKey);
        substitutions.add(substitution);
    }
    

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException(); // TODO
    }

    @Override
    public Collection<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();
        for (Map.Entry<Integer, HashMap<Integer, Set<Integer>>> entry : spo.entrySet()) {
            int sIndex = entry.getKey();
            Term subject = dict.getKey(sIndex);
            for (Map.Entry<Integer, Set<Integer>> entry1 : entry.getValue().entrySet()) {
                Integer pIndex = entry1.getKey();
                Term predicate = dict.getKey(pIndex);
                for (Integer oIndex : entry1.getValue()) {
                    Term object = dict.getKey(oIndex);
                    atoms.add(new RDFAtom(subject, predicate, object));
                }
            }
        }
        return atoms;
    }

    private AtomMatchType determineTermType(Term subject, Term predicate, Term object){
        if (!subject.isVariable() && !predicate.isVariable() && !object.isVariable()) {
            return AtomMatchType.CONST_CONST_CONST;
        }
        else if (!subject.isVariable() && !predicate.isVariable() && object.isVariable()) {
            return AtomMatchType.CONST_CONST_VAR;
        }
        else if (!subject.isVariable() && predicate.isVariable() && !object.isVariable()) {
            return AtomMatchType.CONST_VAR_CONST;
        }
        else if (!subject.isVariable() && predicate.isVariable() && object.isVariable()) {
            return AtomMatchType.CONST_VAR_VAR;
        }
        else if (subject.isVariable() && !predicate.isVariable() && !object.isVariable()) {
            return AtomMatchType.VAR_CONST_CONST;
        }
        else if (subject.isVariable() && !predicate.isVariable() && object.isVariable()) {
            return AtomMatchType.VAR_CONST_VAR;
        }
        else if (subject.isVariable() && predicate.isVariable() && !object.isVariable()) {
            return AtomMatchType.VAR_VAR_CONST;
        }
        else if (subject.isVariable() && predicate.isVariable() && object.isVariable()) {
            return AtomMatchType.VAR_VAR_VAR;
        }
        return null;
    }

    private enum AtomMatchType {
        CONST_CONST_CONST,
        CONST_CONST_VAR,
        CONST_VAR_CONST,
        CONST_VAR_VAR,
        VAR_CONST_CONST,
        VAR_CONST_VAR,
        VAR_VAR_CONST,
        VAR_VAR_VAR
    }
}
