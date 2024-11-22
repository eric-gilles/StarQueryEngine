package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
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
        if (subject.isVariable() || predicate.isVariable() || object.isVariable()) return false;

        Integer sIndex = dict.addAndGet(subject);
        Integer pIndex = dict.addAndGet(predicate);
        Integer oIndex = dict.addAndGet(object);


        if (spo.containsKey(sIndex) && spo.get(sIndex).containsKey(pIndex) && spo.get(sIndex).get(pIndex).contains(oIndex)) return false;

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
        System.out.println(atomMatchType);
        switch (atomMatchType){
            case CONST_CONST_CONST ->  // Subject: Constant, Predicate: Constant, Object: Constant
                matchExact(spo, sIndex, pIndex, oIndex, substitutions);
            case CONST_CONST_VAR -> // Subject: Constant, Predicate: Constant, Object: Variable
                match1Var(spo, sIndex, pIndex, object, substitutions);

            case CONST_VAR_CONST ->  // Subject: Constant, Predicate: Variable, Object: Constant
                match1Var(sop, sIndex, oIndex, predicate, substitutions);

            case CONST_VAR_VAR ->  // Subject: Constant, Predicate: Variable, Object: Variable
                match2Var(spo, sIndex, predicate, object, substitutions);

            case VAR_CONST_CONST ->  // Subject: Variable, Predicate: Constant, Object: Constant
                match1Var(pos, pIndex, oIndex, subject, substitutions);

            case VAR_CONST_VAR -> // Subject: Variable, Predicate: Constant, Object: Variable
                match2Var(pso, pIndex, subject, object, substitutions);

            case VAR_VAR_CONST ->  // Subject: Variable, Predicate: Variable, Object: Constant
                match2Var(ops, oIndex, predicate, subject, substitutions);

            case VAR_VAR_VAR -> // Subject: Variable, Predicate: Variable, Object: Variable
                match3Var(spo, subject, predicate, object, substitutions);

            case null -> {}
            default -> {}
        }
        return substitutions.iterator();
    }
    private void matchExact(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex,
                            Integer thirdIndex, List<Substitution> substitutions) {
        if (
                hashMap.containsKey(firstIndex) &&
                        hashMap.get(firstIndex).containsKey(secondIndex) &&
                        hashMap.get(secondIndex).get(secondIndex).contains(thirdIndex)
        ) {
            substitutions.add(new SubstitutionImpl());
        }
    }

    private void match1Var(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex,
                           Term firstTerm, List<Substitution> substitutions){
        if (hashMap.containsKey(firstIndex)) {
            if (hashMap.get(firstIndex).containsKey(secondIndex)) {
                for (Integer varIndex : hashMap.get(firstIndex).get(secondIndex)) {
                    Substitution sub = new SubstitutionImpl();
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(firstTerm.label()), dict.getKey(varIndex));
                    substitutions.add(sub);
                }
            }
        }
    }
    private void match2Var(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex,
                                         Term firstTerm, Term secondTerm, List<Substitution> substitutions) {
        if (hashMap.containsKey(firstIndex)) {
            for (Map.Entry<Integer, Set<Integer>> entry : hashMap.get(firstIndex).entrySet()) {
                Integer firstVarIndex = entry.getKey();
                for (Integer secondVarIndex : entry.getValue()) {
                    Substitution sub = new SubstitutionImpl();
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(firstTerm.label()), dict.getKey(firstVarIndex));
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(secondTerm.label()), dict.getKey(secondVarIndex));
                    substitutions.add(sub);
                }
            }
        }

    }
    private void match3Var(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap,
                           Term firstTerm, Term secondTerm, Term thirdTerm, List<Substitution> substitutions) {
        for (Map.Entry<Integer, HashMap<Integer, Set<Integer>>> entry : hashMap.entrySet()) {
            Integer s = entry.getKey();
            for (Map.Entry<Integer, Set<Integer>> entry1 : entry.getValue().entrySet()) {
                Integer p = entry1.getKey();
                for (Integer o : entry1.getValue()) {
                    Substitution sub = new SubstitutionImpl();
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(firstTerm.label()), dict.getKey(s));
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(secondTerm.label()), dict.getKey(p));
                    sub.add(SameObjectTermFactory.instance().createOrGetVariable(thirdTerm.label()), dict.getKey(o));
                    substitutions.add(sub);
                }
            }
        }

    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        List<Substitution> substitutions = new ArrayList<>();

        for (RDFAtom atom : q.getRdfAtoms()) {
            Iterator<Substitution> matchedAtoms = this.match(atom);
            List<Substitution> matchedList = new ArrayList<>();
            matchedAtoms.forEachRemaining(matchedList::add);

            // Fusionner les substitutions existantes avec les nouvelles
            //substitutions = merge(substitutions, matchedList);
        }
        return substitutions.iterator();
    }

    public List<Substitution> merge(List<Substitution> listA, List<Substitution> listB) {
        List<Substitution> mergedList = new ArrayList<>(listA);
        for(Substitution subA : listA){
            for(Substitution subB: listB){
                if(hasCommonVariables(subA, subB)){
                    Optional<Substitution> mergedSub = subA.merged(subB);
                } else {
                    //Faut voir avec le reste des substitutions si aucune n'est commune
                }
            }
        }
        return mergedList;
    }

    // Vérifie si deux substitutions partagent des variables communes
    private boolean hasCommonVariables(Substitution sub1, Substitution sub2) {
        for (Variable var : sub1.keys()) {
            if (sub2.keys().contains(var)) {
                return true;
            }
        }
        return false;
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
        if (!subject.isVariable() && !predicate.isVariable() && !object.isVariable()) { //Const Const Const
            return AtomMatchType.CONST_CONST_CONST;
        }
        else if (!subject.isVariable() && !predicate.isVariable() && object.isVariable()) { //Const Const Var
            return AtomMatchType.CONST_CONST_VAR;
        }
        else if (!subject.isVariable() && predicate.isVariable() && !object.isVariable()) { //Const Var Const
            return AtomMatchType.CONST_VAR_CONST;
        }
        else if (!subject.isVariable() && predicate.isVariable() && object.isVariable()) { //Const Var Var
            return AtomMatchType.CONST_VAR_VAR;
        }
        else if (subject.isVariable() && !predicate.isVariable() && !object.isVariable()) { //Var Const Const
            return AtomMatchType.VAR_CONST_CONST;
        }
        else if (subject.isVariable() && !predicate.isVariable() && object.isVariable()) { //Var Const Var
            return AtomMatchType.VAR_CONST_VAR;
        }
        else if (subject.isVariable() && predicate.isVariable() && !object.isVariable()) { //Var Var Const
            return AtomMatchType.VAR_VAR_CONST;
        }
        else if (subject.isVariable() && predicate.isVariable() && object.isVariable()) { //Var Var Var
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
