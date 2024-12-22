package qengine_concurrent.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import qengine_concurrent.model.RDFAtom;
import qengine_concurrent.model.StarQuery;
import qengine_concurrent.storage.RDFStorage;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private BidiMap<Integer, Term> dict = new DualHashBidiMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesSPO = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesSOP = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesPSO = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesPOS = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesOSP = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesOPS = new HashMap<>();
    private int dictIndex = 1;
    private int size = 0;

    @Override
    public boolean add(RDFAtom atom) {
        var terms = List.of(atom.getTripleSubject(), atom.getTriplePredicate(), atom.getTripleObject());
        Integer[] indexes = new Integer[3];
        for (int i = 0; i < 3; i++) {
            if (dict.containsValue(terms.get(i))) {
                indexes[i] = dict.inverseBidiMap().get(terms.get(i));
            } else {
                dict.put(dictIndex, terms.get(i));
                indexes[i] = dictIndex;
                dictIndex++;
            }
        }

        // Vérifie si l'atome est déjà enregistré
        if (atomIndexesSPO.containsKey(indexes[0]) && atomIndexesSPO.get(indexes[0]).containsKey(indexes[1]) && atomIndexesSPO.get(indexes[0]).get(indexes[1]).contains(indexes[2])) {
            return false;
        }

        size++;

        // S = 0 | P = 1 | O = 2
        addIndex(atomIndexesSPO, indexes[0], indexes[1], indexes[2]);
        addIndex(atomIndexesSOP, indexes[0], indexes[2], indexes[1]);
        addIndex(atomIndexesPSO, indexes[1], indexes[0], indexes[2]);
        addIndex(atomIndexesPOS, indexes[1], indexes[2], indexes[0]);
        addIndex(atomIndexesOSP, indexes[2], indexes[0], indexes[1]);
        addIndex(atomIndexesOPS, indexes[2], indexes[1], indexes[0]);

        return true;
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

        Set<Substitution> substitutions = new HashSet<>();

        MatchAtomCase matchAtomCase = getMatchAtomCase(subject, predicate, object);

        switch (matchAtomCase) {
            case CONST_CONST_CONST -> {}
            case CONST_CONST_VAR -> {
                Integer subjectIndex = dict.inverseBidiMap().get(subject);
                Integer predicateIndex = dict.inverseBidiMap().get(predicate);

                // Vérifie que les index existent dans atomIndexesSPO
                if (subjectIndex != null && predicateIndex != null &&
                        atomIndexesSPO.containsKey(subjectIndex) &&
                        atomIndexesSPO.get(subjectIndex).containsKey(predicateIndex)) {

                    // Obtenir l'ensemble des objets pour ce sujet et prédicat
                    Set<Integer> objectIndexes = atomIndexesSPO.get(subjectIndex).get(predicateIndex);

                    // Créer des substitutions pour chaque objet trouvé
                    for (Integer objectIndex : objectIndexes) {
                        Term matchedObject = dict.get(objectIndex);
                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) object, matchedObject); // Associer la variable objet avec la valeur trouvée
                        substitutions.add(substitution);
                    }
                }
            }
            case CONST_VAR_CONST -> {
                Integer subjectIndex = dict.inverseBidiMap().get(subject);
                Integer objectIndex = dict.inverseBidiMap().get(object);

                // Vérifie que les index existent dans atomIndexesSOP
                if (subjectIndex != null && objectIndex != null &&
                        atomIndexesSOP.containsKey(subjectIndex) &&
                        atomIndexesSOP.get(subjectIndex).containsKey(objectIndex)) {

                    // Obtenir l'ensemble des prédicats pour ce sujet et objet
                    Set<Integer> predicateIndexes = atomIndexesSOP.get(subjectIndex).get(objectIndex);

                    // Créer des substitutions pour chaque prédicat trouvé
                    for (Integer predicateIndex : predicateIndexes) {
                        Term matchedPredicate = dict.get(predicateIndex);
                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) predicate, matchedPredicate); // Associer la variable prédicat avec la valeur trouvée
                        substitutions.add(substitution);
                    }
                }
            }
            case CONST_VAR_VAR -> {
                Integer subjectIndex = dict.inverseBidiMap().get(subject);

                // Vérifie que l'index existe dans atomIndexesSPO pour le sujet donné
                if (subjectIndex != null && atomIndexesSPO.containsKey(subjectIndex)) {
                    Map<Integer, Set<Integer>> predicatesMap = atomIndexesSPO.get(subjectIndex);

                    // Parcourir tous les prédicats pour le sujet constant
                    for (Map.Entry<Integer, Set<Integer>> entry : predicatesMap.entrySet()) {
                        Integer predicateIndex = entry.getKey();
                        Set<Integer> objectIndexes = entry.getValue();

                        // Parcourir tous les objets pour chaque prédicat
                        for (Integer objectIndex : objectIndexes) {
                            Term matchedPredicate = dict.get(predicateIndex);
                            Term matchedObject = dict.get(objectIndex);

                            // Créer une substitution pour chaque couple (prédicat, objet) trouvé
                            Substitution substitution = new SubstitutionImpl();
                            substitution.add((Variable) predicate, matchedPredicate);
                            substitution.add((Variable) object, matchedObject);
                            substitutions.add(substitution);
                        }
                    }
                }
            }
            case VAR_CONST_CONST -> {
                Integer predicateIndex = dict.inverseBidiMap().get(predicate);
                Integer objectIndex = dict.inverseBidiMap().get(object);

                // Vérifie que les index existent dans atomIndexesPOS
                if (predicateIndex != null && objectIndex != null &&
                        atomIndexesPOS.containsKey(predicateIndex) &&
                        atomIndexesPOS.get(predicateIndex).containsKey(objectIndex)) {

                    // Obtenir l'ensemble des sujets pour ce prédicat et objet
                    Set<Integer> subjectIndexes = atomIndexesPOS.get(predicateIndex).get(objectIndex);

                    // Créer des substitutions pour chaque sujet trouvé
                    for (Integer subjectIndex : subjectIndexes) {
                        Term matchedSubject = dict.get(subjectIndex);
                        Substitution substitution = new SubstitutionImpl();
                        substitution.add((Variable) subject, matchedSubject); // Associer la variable sujet avec la valeur trouvée
                        substitutions.add(substitution);
                    }
                }
            }
            case VAR_CONST_VAR -> {
                Integer predicateIndex = dict.inverseBidiMap().get(predicate);

                // Vérifie que l'index existe dans atomIndexesPSO pour le prédicat donné
                if (predicateIndex != null && atomIndexesPSO.containsKey(predicateIndex)) {
                    Map<Integer, Set<Integer>> subjectsMap = atomIndexesPSO.get(predicateIndex);

                    // Parcourir tous les sujets pour le prédicat constant
                    for (Map.Entry<Integer, Set<Integer>> entry : subjectsMap.entrySet()) {
                        Integer subjectIndex = entry.getKey();
                        Set<Integer> objectIndexes = entry.getValue();

                        // Parcourir tous les objets pour chaque sujet
                        for (Integer objectIndex : objectIndexes) {
                            Term matchedSubject = dict.get(subjectIndex);
                            Term matchedObject = dict.get(objectIndex);

                            // Créer une substitution pour chaque couple (sujet, objet) trouvé
                            Substitution substitution = new SubstitutionImpl();
                            substitution.add((Variable) subject, matchedSubject);
                            substitution.add((Variable) object, matchedObject);
                            substitutions.add(substitution);
                        }
                    }
                }
            }
            case VAR_VAR_CONST -> {
                Integer objectIndex = dict.inverseBidiMap().get(object);

                // Vérifie que l'index existe dans atomIndexesOSP pour l'objet donné
                if (objectIndex != null && atomIndexesOSP.containsKey(objectIndex)) {
                    Map<Integer, Set<Integer>> subjectsMap = atomIndexesOSP.get(objectIndex);

                    // Parcourir tous les sujets pour l'objet constant
                    for (Map.Entry<Integer, Set<Integer>> entry : subjectsMap.entrySet()) {
                        Integer subjectIndex = entry.getKey();
                        Set<Integer> predicateIndexes = entry.getValue();

                        // Parcourir tous les prédicats pour chaque sujet
                        for (Integer predicateIndex : predicateIndexes) {
                            Term matchedSubject = dict.get(subjectIndex);
                            Term matchedPredicate = dict.get(predicateIndex);

                            // Créer une substitution pour chaque couple (sujet, prédicat) trouvé
                            Substitution substitution = new SubstitutionImpl();
                            substitution.add((Variable) subject, matchedSubject);
                            substitution.add((Variable) predicate, matchedPredicate);
                            substitutions.add(substitution);
                        }
                    }
                }
            }
            case VAR_VAR_VAR -> {
                // Parcourir tous les sujets dans atomIndexesSPO
                for (Map.Entry<Integer, Map<Integer, Set<Integer>>> subjectEntry : atomIndexesSPO.entrySet()) {
                    Integer subjectIndex = subjectEntry.getKey();
                    Term matchedSubject = dict.get(subjectIndex);

                    // Parcourir tous les prédicats pour chaque sujet
                    for (Map.Entry<Integer, Set<Integer>> predicateEntry : subjectEntry.getValue().entrySet()) {
                        Integer predicateIndex = predicateEntry.getKey();
                        Term matchedPredicate = dict.get(predicateIndex);

                        // Parcourir tous les objets pour chaque prédicat
                        for (Integer objectIndex : predicateEntry.getValue()) {
                            Term matchedObject = dict.get(objectIndex);

                            // Créer une substitution pour chaque triplet (sujet, prédicat, objet) trouvé
                            Substitution substitution = new SubstitutionImpl();
                            substitution.add((Variable) subject, matchedSubject);
                            substitution.add((Variable) predicate, matchedPredicate);
                            substitution.add((Variable) object, matchedObject);
                            substitutions.add(substitution);
                        }
                    }
                }
            }
        }

        // Retourner un itérateur sur les substitutions trouvées
        return substitutions.iterator();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        Set<Substitution> substitutions = new HashSet<>();

        for (RDFAtom atom : q.getRdfAtoms()) {
            Iterator<Substitution> matchIterator = match(atom);
            Set<Substitution> intersectedSubstitutions = new HashSet<>();

            while (matchIterator.hasNext()) {
                Substitution substitution = matchIterator.next();
                if (substitutions.contains(substitution) || q.getRdfAtoms().getFirst().equals(atom)) {
                    intersectedSubstitutions.add(substitution);
                }
            }
            substitutions = intersectedSubstitutions;
        }

        return substitutions.iterator();
    }

    @Override
    public List<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();

        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> subjectEntry : atomIndexesSPO.entrySet()) {
            int subjectIndex = subjectEntry.getKey();
            Term subject = dict.get(subjectIndex);

            for (Map.Entry<Integer, Set<Integer>> predicateEntry : subjectEntry.getValue().entrySet()) {
                int predicateIndex = predicateEntry.getKey();
                Term predicate = dict.get(predicateIndex);

                for (Integer objectIndex : predicateEntry.getValue()) {
                    Term object = dict.get(objectIndex);

                    // Créer un nouvel atome RDF avec le sujet, prédicat et objet récupérés
                    RDFAtom atom = new RDFAtom(subject, predicate, object);
                    atoms.add(atom);
                }
            }
        }

        return atoms;
    }

    @Override
    public String toString() {
        return "HexaStore"
                + "\n|_ Dictionary: " + dict.toString()
                + "\n|_ Atom indexes (SPO): " + atomIndexesSPO.toString()
                + "\n|_ Atom indexes (SOP): " + atomIndexesSOP.toString()
                + "\n|_ Atom indexes (PSO): " + atomIndexesPSO.toString()
                + "\n|_ Atom indexes (POS): " + atomIndexesPOS.toString()
                + "\n|_ Atom indexes (OSP): " + atomIndexesOSP.toString()
                + "\n|_ Atom indexes (OPS): " + atomIndexesOPS.toString();
    }

    private enum MatchAtomCase {
        CONST_CONST_CONST,
        CONST_CONST_VAR,
        CONST_VAR_CONST,
        CONST_VAR_VAR,
        VAR_CONST_CONST,
        VAR_CONST_VAR,
        VAR_VAR_CONST,
        VAR_VAR_VAR,
    }

    private MatchAtomCase getMatchAtomCase(Term subject, Term predicate, Term object) {
        if (!subject.isVariable() && !predicate.isVariable() && !object.isVariable()) {
            return MatchAtomCase.CONST_CONST_CONST;
        }
        if (!subject.isVariable() && !predicate.isVariable() && object.isVariable()) {
            return MatchAtomCase.CONST_CONST_VAR;
        }
        if (!subject.isVariable() && predicate.isVariable() && !object.isVariable()) {
            return MatchAtomCase.CONST_VAR_CONST;
        }
        if (!subject.isVariable() && predicate.isVariable() && object.isVariable()) {
            return MatchAtomCase.CONST_VAR_VAR;
        }
        if (subject.isVariable() && !predicate.isVariable() && !object.isVariable()) {
            return MatchAtomCase.VAR_CONST_CONST;
        }
        if (subject.isVariable() && !predicate.isVariable() && object.isVariable()) {
            return MatchAtomCase.VAR_CONST_VAR;
        }
        if (subject.isVariable() && predicate.isVariable() && !object.isVariable()) {
            return MatchAtomCase.VAR_VAR_CONST;
        }
        return MatchAtomCase.VAR_VAR_VAR;
    }

    private void addIndex(Map<Integer, Map<Integer, Set<Integer>>> atomIndexes, int x, int y, int z) {
        if (atomIndexes.containsKey(x)) {
            if (atomIndexes.get(x).containsKey(y)) {
                atomIndexes.get(x).get(y).add(z);
            } else {
                atomIndexes.get(x).put(y, new HashSet<>());
                atomIndexes.get(x).get(y).add(z);
            }
        } else {
            atomIndexes.put(x, new HashMap<>());
            atomIndexes.get(x).put(y, new HashSet<>());
            atomIndexes.get(x).get(y).add(z);
        }
    }

}
