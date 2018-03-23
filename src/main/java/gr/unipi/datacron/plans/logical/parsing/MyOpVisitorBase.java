/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.parsing;

import gr.unipi.datacron.plans.logical.parsing.operators.*;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;

import java.util.*;
import java.util.stream.Collectors;

/**
 *
 * @author nicholaskoutroumanis
 */
public class MyOpVisitorBase extends OpVisitorBase {

    /**
     * @return the bop
     */
    public BaseOperator[] getBop() {
        return bop;
    }

    private BaseOperator[] bop;

    private MyOpVisitorBase(String sparql) {
        getTriples(sparql);
    }

    private void myOpVisitorWalker(Op op) {
        OpWalker.walk(op, this);
    }

    @Override
    public void visit(final OpBGP opBGP) {

        List<Triple> triples = opBGP.getPattern().getList();

        List<FilterOf> listOfFilters = new ArrayList<>();

        triples.forEach((triple) -> {
            //form the list with the correct form of Subject, Predicate, Object

            String subject = (triple.getSubject().toString().substring(0, 1).equals("?")) ? triple.getSubject().toString() : triple.getSubject().toString().substring(1, triple.getSubject().toString().length() - 1);
            String predicate = (triple.getPredicate().toString().substring(1, 2).equals("?")) ? triple.getPredicate().toString() : (triple.getPredicate().toString().substring(1, 2).equals(":")) ? triple.getPredicate().toString().substring(1) : triple.getPredicate().toString().substring(triple.getPredicate().toString().lastIndexOf("/") + 1);
            String object = (triple.getObject().toString().substring(0, 1).equals("?")) ? triple.getObject().toString() : triple.getObject().toString().substring(1, triple.getObject().toString().length() - 1);

            TripleOperator to = TripleOperator.newTripleOperator(subject, predicate, object);

            Map<Column, Column> hm = new LinkedHashMap<>();

            for (Column c : to.getArrayColumns()) {
                hm.put(c, c.copyToNewObject(Integer.toString(to.hashCode())));
            }

            ProjectOperator p = ProjectOperator.newProjectOperator(to, hm);

            List<ColumnWithValue> k = new ArrayList<>();
            for (Column c : p.getArrayColumns()) {
                if (!(c instanceof ColumnWithVariable)) {
                    k.add(ColumnWithValue.newColumnWithValue(c, c.getQueryString()));
                }
            }

            listOfFilters.add(FilterOf.newFilterOf(p, p.getArrayColumns(), k.stream().toArray(ColumnWithValue[]::new)));
        });

        formBaseOperatorArray(formStarQueriesAndRemainingTriplets(checkForShortcuts(listOfFilters)));
    }

    private void getTriples(String q) {
        Query query = QueryFactory.create(q);
        Op op = Algebra.compile(query);
        this.myOpVisitorWalker(op);

    }

    private List<BaseOperator> formStarQueriesAndRemainingTriplets(List<FilterOf> listOfFilters) {

        Set<Integer> excludedElements = new HashSet<>();

        List<BaseOperator> starQueryTreeList = new ArrayList<>();

        for (int i = 0; i < listOfFilters.size(); i++) {

            if (excludedElements.contains(i)) {
                continue;
            }

            String choosenSubject;
            if (!listOfFilters.get(i).isSubjectVariable()) {
                break;
            } else {
                choosenSubject = listOfFilters.get(i).getSubject();
            }

            List<BaseOperator> aStarQueryTripletsList = new ArrayList<>();
            aStarQueryTripletsList.add(listOfFilters.get(i));

            for (int k = i + 1; k < listOfFilters.size(); k++) {

                // it was if (excludedElements.contains(i)) {
                if (excludedElements.contains(k)) {
                    continue;
                }

                if (choosenSubject.equals(listOfFilters.get(k).getSubject())) {
                    excludedElements.add(k);
                    aStarQueryTripletsList.add(listOfFilters.get(k));
                }
            }

            if (aStarQueryTripletsList.size() > 1) {
                excludedElements.add(i);
                JoinOrOperator orp = JoinOrOperator.newJoinOrOperator(aStarQueryTripletsList.stream().toArray(BaseOperator[]::new));
                starQueryTreeList.add(orp);
            }
        }

        //remove the triplets from the list that are part of star query - the remaining only triplets will be maintained
        List<Integer> l = new ArrayList<>(excludedElements);
        Collections.reverse(l);

        l.forEach((i)
                -> listOfFilters.remove(i.intValue())
        );

        starQueryTreeList.addAll(listOfFilters);

        //return a list with the JoinOrOperators and FilterOf Operators 
        return starQueryTreeList;
    }

    private void formBaseOperatorArray(List<BaseOperator> l) {

        Set<Integer> excludedFromList = new HashSet<>();

        List<BaseOperator> bopList = new ArrayList<>();

        for (int i = 0; i < l.size(); i++) {

            if (excludedFromList.contains(i)) {
                continue;
            }

            BaseOperator choosenBop = l.get(i);

            int k = i + 1;
            while (k < l.size()) {

                if (excludedFromList.contains(k)) {
                    k++;
                    continue;
                }

                if (choosenBop.hasCommonVariable(l.get(k))) {
                    choosenBop = JoinOperator.newJoinOperator(choosenBop, l.get(k));
                    excludedFromList.add(k);
                    k = i + 1;
                } else {
                    k++;
                }
            }
            excludedFromList.add(i);
            bopList.add(choosenBop);
        }

        bop = bopList.stream().toArray(BaseOperator[]::new);

    }

    private List<FilterOf> checkForShortcuts(List<FilterOf> l) {

        final Set<String> set = new HashSet<>(Arrays.asList(":hasGeometry", ":hasWKT", ":hasTemporalFeature", ":TimeStart"));

        Map<String[], String> hashMap = new HashMap<>();
        hashMap.put(new String[]{":hasGeometry", ":hasWKT"}, ":hasWKTGeom");
        hashMap.put(new String[]{":hasTemporalFeature", ":TimeStart"}, "something");

        //word - The list which contains the word as a predicate
        Map<String, List<FilterOf>> p = l.stream().filter(f
                -> (!f.isPredicateVariable()) ? (set.contains(f.getPredicate()) && (f.isSubjectVariable() || f.isObjectVariable())) : false
        ).collect(Collectors.groupingBy(ba -> ba.getPredicate()));

        hashMap.forEach((k, v) -> {
            if (p.containsKey(k[0]) && p.containsKey(k[1])) {

                List<FilterOf> list1 = p.get(k[0]);
                List<FilterOf> list2 = p.get(k[1]);

                for (FilterOf l1 : list1) {
                    int i = 0;
                    while (i < list2.size()) {
                        if (l1.getObject().equals(list2.get(i).getSubject())) {

                            TripleOperator to = TripleOperator.newTripleOperator(l1.getSubject(), v, list2.get(i).getObject());

                            Map<Column, Column> hm = new LinkedHashMap<>();

                            for (Column c : to.getArrayColumns()) {
                                hm.put(c, c.copyToNewObject(Integer.toString(to.hashCode())));
                            }

                            ProjectOperator pop = ProjectOperator.newProjectOperator(to, hm);

                            List<ColumnWithValue> cwv = new ArrayList<>();
                            for (Column c : pop.getArrayColumns()) {
                                if (!(c instanceof ColumnWithVariable)) {
                                    cwv.add(ColumnWithValue.newColumnWithValue(c, c.getQueryString()));
                                }
                            }

                            l.set(l.indexOf(l1), FilterOf.newFilterOf(pop, pop.getArrayColumns(), cwv.stream().toArray(ColumnWithValue[]::new)));

                            l.remove(list2.get(i));

                            list2.remove(i);
                            break;

                        }
                        i++;
                    }
                }
            }
        });

        return l;
    }

    public static MyOpVisitorBase newMyOpVisitorBase(String sparql) {
        return new MyOpVisitorBase(sparql);

    }
}
