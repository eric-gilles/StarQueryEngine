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

    /**
     * Ajoute un RDFAtom à l'HexaStore.
     * @param atom le RDFAtom à ajouter
     * @return true si l'ajout a réussi, false sinon
     */
    @Override
    public boolean add(RDFAtom atom) {
        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        if (subject.isVariable() || predicate.isVariable() || object.isVariable()) return false;

        Integer sIndex = dict.addAndGet(subject);
        Integer pIndex = dict.addAndGet(predicate);
        Integer oIndex = dict.addAndGet(object);


        if (spo.containsKey(sIndex) && spo.get(sIndex).containsKey(pIndex) && spo.get(sIndex).get(pIndex).contains(oIndex))
            return false;

        size++;

        return addToAllIndex(sIndex, pIndex, oIndex);
    }


    /**
     * Méthode pour ajouter un sujet, un prédicat et un objet à tous les index.
     * Méthode utilisée dans la méthode add (RDFAtom atom) pour ajouter un RDFAtom à l'HexaStore.
     *
     * @param sIndex l'index du sujet
     * @param pIndex l'index du prédicat
     * @param oIndex l'index de l'objet
     * @return true si l'ajout a réussi pour tous les index, false sinon
     */
    private boolean addToAllIndex(Integer sIndex, Integer pIndex, Integer oIndex) {
        return addToIndex(spo, sIndex, pIndex, oIndex) &&
                addToIndex(pso, pIndex, sIndex, oIndex) &&
                addToIndex(osp, oIndex, sIndex, pIndex) &&
                addToIndex(pos, pIndex, oIndex, sIndex) &&
                addToIndex(sop, sIndex, oIndex, pIndex) &&
                addToIndex(ops, oIndex, pIndex, sIndex);
    }

    /**
     * Méthode pour ajouter un index dans une hashmap de l'HexaStore.
     * Méthode utilisée dans la méthode 'addToAllIndex'.
     *
     * @param hashMap la hashmap à laquelle ajouter l'index
     * @param firstIndex l'index du premier terme
     * @param secondIndex l'index du deuxième terme
     * @param thirdIndex l'index du troisième terme
     * @return true si l'ajout a réussi, false sinon
     */
     private boolean addToIndex(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex, Integer thirdIndex) {
        return hashMap.computeIfAbsent(firstIndex, k -> new HashMap<>())
                .computeIfAbsent(secondIndex, k -> new HashSet<>())
                .add(thirdIndex);
    }

    /**
     * Retourne le nombre d'atomes dans l'HexaStore.
     *
     * @return size le nombre d'atomes
     */
     @Override
    public long size() {
        return size;
    }

    /**
     * Retourne un itérateur de substitutions correspondant au match des atomes.
     *
     * @param atom l'atome à matcher
     * @return un itérateur de substitutions
     */
    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        List<Substitution> substitutions = new ArrayList<>();

        Integer sIndex = dict.get(subject);
        Integer pIndex = dict.get(predicate);
        Integer oIndex = dict.get(object);

        switch (determineTermType(subject, predicate, object)) {
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

            case null, default -> {}
        }
        return substitutions.iterator();
    }

    /**
     * Méthode pour matcher un atome avec des termes constants utilisée dans la méthode 'match'.
     *
     * @param hashMap HashMap de l'HexaStore à matcher
     * @param firstIndex l'index du premier terme
     * @param secondIndex l'index du deuxième terme
     * @param thirdIndex l'index du troisième terme
     * @param substitutions la liste de substitutions
     */
    private void matchExact(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex,
                            Integer thirdIndex, List<Substitution> substitutions) {
        if (hashMap.containsKey(firstIndex) && hashMap.get(firstIndex).containsKey(secondIndex) &&
                hashMap.get(firstIndex).get(secondIndex).contains(thirdIndex)) {
            substitutions.add(new SubstitutionImpl());
        }
    }

    /**
     * Méthode pour matcher un atome avec un terme constant et une variable utilisée dans la méthode 'match'.
     *
     * @param hashMap HashMap de l'HexaStore à matcher
     * @param firstIndex l'index du premier terme
     * @param secondIndex l'index du deuxième terme
     * @param firstTerm le premier terme
     * @param substitutions la liste de substitutions
     */
    private void match1Var(HashMap<Integer, HashMap<Integer, Set<Integer>>> hashMap, Integer firstIndex, Integer secondIndex,
                           Term firstTerm, List<Substitution> substitutions) {
        if (hashMap.containsKey(firstIndex) && hashMap.get(firstIndex).containsKey(secondIndex)) {
            for (Integer varIndex : hashMap.get(firstIndex).get(secondIndex)) {
                Substitution sub = new SubstitutionImpl();
                sub.add(SameObjectTermFactory.instance().createOrGetVariable(firstTerm.label()), dict.getKey(varIndex));
                substitutions.add(sub);
            }
        }
    }

    /**
     * Méthode pour matcher un atome avec deux variables utilisée dans la méthode 'match'.
     *
     * @param hashMap HashMap de l'HexaStore à matcher
     * @param firstIndex l'index du premier terme
     * @param firstTerm le premier terme
     * @param secondTerm le deuxième terme
     * @param substitutions la liste de substitutions
     */
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

    /**
     * Méthode pour matcher un atome avec trois variables utilisée dans la méthode 'match'.
     *
     * @param hashMap HashMap de l'HexaStore à matcher
     * @param firstTerm le premier terme
     * @param secondTerm le deuxième terme
     * @param thirdTerm le troisième terme
     * @param substitutions la liste de substitutions
     */
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

    /**
     * Méthode pour déterminer le type de termes utilisée dans la méthode 'match'.
     *
     * @param subject le sujet de l'atome RDF
     * @param predicate le prédicat de l'atome RDF
     * @param object l'objet de l'atome RDF
     * @return AtomMatchType
     */
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
        else return AtomMatchType.VAR_VAR_VAR; // Var Var Var
    }

    /**
     * Enumération des types de match d'atomes.
     */
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

    /**
     * Retourne un itérateur de substitutions correspondant au match des atomes d'une requête en étoile.
     *
     * @param query la requête en étoile à matcher
     * @return un itérateur de substitutions
     */
    @Override
    public Iterator<Substitution> match(StarQuery query) {
        List<Substitution> substitutions = new ArrayList<>();

        for (RDFAtom atom : query.getRdfAtoms()) {
            Iterator<Substitution> matchedAtoms = this.match(atom);
            List<Substitution> matchedList = new ArrayList<>();
            matchedAtoms.forEachRemaining(matchedList::add);
            substitutions = mergeGeneral(substitutions, matchedList);

        }
        if (substitutions.isEmpty()) substitutions.add(new SubstitutionImpl());
        return substitutions.iterator();
    }

    /**
     * Méthode pour fusionner des substitutions.
     * Méthode utilisée dans la méthode 'match(query)'.
     *
     * @param substitutions la liste de substitutions
     * @param subFromAtom la liste de substitutions à fusionner
     * @return la liste de substitutions fusionnée
     */
    public List<Substitution> mergeGeneral(List<Substitution> substitutions, List<Substitution> subFromAtom) {
        ArrayList<Substitution> res = new ArrayList<>();

        // Cas trivial :
        if (substitutions.isEmpty()) return subFromAtom;

        // Parcourir toutes les combinaisons
        for (Substitution subA : subFromAtom) {
            for (Substitution subB : substitutions) {
                Map<Variable, Term> mapSubA = subA.toMap();
                Map<Variable, Term> mapSubB = subB.toMap();
                Map<Variable, Term> mergedMap = new HashMap<>(mapSubA); //Map qui stock les variables pour la fusion

                boolean isCompatible = true;

                // Vérifier la compatibilité avec les variables communes
                for (Map.Entry<Variable, Term> entry : mapSubB.entrySet()) {
                    Variable key = entry.getKey();
                    Term value = entry.getValue();

                    // Si la variable est commune, vérifier la compatibilité
                    if (mapSubA.containsKey(key) && !mapSubA.get(key).equals(value)) {
                        isCompatible = false; // Conflit détecté
                        break;
                    } else {
                        // Sinon, ajouter la variable dans la fusion (pas de conflit)
                        mergedMap.put(key, value);
                    }
                }

                if (isCompatible) {
                    // Créer une nouvelle substitution à partir de la fusion
                    Substitution mergedSub = new SubstitutionImpl();
                    mergedMap.forEach(mergedSub::add);
                    res.add(mergedSub);
                }
            }
        }
        return res;
    }


    /**
     * Retourne une collection contenant tous les atomes de l'HexaStore.
     *
     * @return une collection d'atomes
     */
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
}
