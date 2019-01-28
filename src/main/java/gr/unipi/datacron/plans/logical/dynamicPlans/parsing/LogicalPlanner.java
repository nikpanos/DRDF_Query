/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gr.unipi.datacron.plans.logical.dynamicPlans.parsing;

import gr.unipi.datacron.common.AppConfig;
import gr.unipi.datacron.common.Consts;
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.*;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.BaseOperand;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.ColumnOperand;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.OperandPair;
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.ValueOperand;
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.*;
import gr.unipi.datacron.store.DataStore;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.SortCondition;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.expr.Expr;
import scala.Option;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author nicholaskoutroumanis
 */
public class LogicalPlanner extends OpVisitorBase {

    public BaseOperator getRoot() {
        return root;
    }

    private BaseOperator root;

    private final boolean optimized;

    private List<String> selectVariables = new ArrayList<>();

    private List<Expr> filters;

    private static int getOptimizationFlag() {
        return AppConfig.getInt(Consts.qfpLogicalOptimizationFlag());
    }


//    private LogicalPlanner(String sparql) {
//        getTriples(sparql);
//    }

    private void myOpVisitorWalker(Op op) {
        OpWalker.walk(op, this);
    }

    private static Long getRedisEncodedValue(String key) {
//        Option<Object> optionValue = DataStore.dictionaryRedis().getEncodedValue(key);
//        Long value = null;
//        if (optionValue.isDefined()) {
//            value = (Long) optionValue.get();
//        }
//        return value;
        return -1L;
    }


    private Long getStatisticsValue(String key) {
        return 8L;//Long.parseLong(DataStore.statisticsRedis().getValue(key).get());
    }

    private static String getRedisDecodedValue(Long key) {
        Option<String> optionValue = DataStore.dictionaryRedis().getDecodedValue(key);
        String value = null;
        if (optionValue.isDefined()) {
            value = optionValue.get();
        }
        return value;
    }

    @Override
    public void visit(final OpProject opProject) {
        opProject.getVars().forEach(e -> selectVariables.add(e.toString()));

    }

    @Override
    public void visit(final OpFilter op) {
        filters = op.getExprs().getList();
    }


    private Long getOutputSize(String subject, String predicate, String object) {

//        String subjectEnc = "";
//        String predicateEnc = "";
//        String objectEnc = "";
//
//        boolean subIsValue = false;
//        boolean predIsValue = false;
//        boolean objIsValue = false;
//
//
//        if (!subject.substring(0, 1).equals("?")) {
//            subjectEnc = getRedisEncodedValue(subject).toString();
//            subIsValue = true;
//        }
//
//        if (!predicate.substring(0, 1).equals("?")) {
//            predicateEnc = getRedisEncodedValue(predicate).toString();
//            predIsValue = true;
//
//        }
//
//        if (!object.substring(0, 1).equals("?")) {
//            objectEnc = getRedisEncodedValue(object).toString();
//            objIsValue = true;
//        }
//
//        Long outputSize;
//        Long numberOfCellsPerAxis = 1000L;
//
//        if (subIsValue && predIsValue) {
//            if (Integer.parseInt(subjectEnc) < 0) {
//                Long groupId = (((Long.parseLong(subjectEnc) - getStatisticsValue("minSub")) * numberOfCellsPerAxis / ((getStatisticsValue("maxNegSub") + 1L) - getStatisticsValue("minSub"))) + (((Long.parseLong(predicateEnc) - getStatisticsValue("minPred")) * numberOfCellsPerAxis / (getStatisticsValue("maxPred") - getStatisticsValue("minPred"))) * numberOfCellsPerAxis));
//                outputSize = getStatisticsValue("spn." + numberOfCellsPerAxis + "." + groupId);
//            } else {
//                outputSize = getStatisticsValue("spp.1.0");
//            }
//        } else if (predIsValue && objIsValue) {
//            if (Integer.parseInt(objectEnc) < 0) {
//                Long groupId = (((Long.parseLong(objectEnc) - getStatisticsValue("minObj")) * numberOfCellsPerAxis / ((getStatisticsValue("maxNegObj") + 1L) - getStatisticsValue("minObj"))) + (((Long.parseLong(predicateEnc) - getStatisticsValue("minPred")) * numberOfCellsPerAxis / (getStatisticsValue("maxPred") - getStatisticsValue("minPred"))) * numberOfCellsPerAxis));
//                outputSize = getStatisticsValue("opn." + numberOfCellsPerAxis + "." + groupId);
//            } else {
//                outputSize = getStatisticsValue("opp.1.0");
//            }
//        } else {
//            Long groupId = (((Long.parseLong(predicateEnc) - getStatisticsValue("minPred")) * numberOfCellsPerAxis / ((getStatisticsValue("maxPred") + 1L) - getStatisticsValue("minPred"))));
//            outputSize = getStatisticsValue("p." + numberOfCellsPerAxis + "." + groupId);
//        }


        return 4L/*outputSize*/;
    }

    @Override
    public void visit(final OpBGP opBGP) {

        List<Triple> triples = opBGP.getPattern().getList();

        List<SelectOperator> listOfSelectOperators = new ArrayList<>();

        triples.forEach((triple) -> {
            //form the list with the correct form of Subject, Predicate, Object

            String subject = (triple.getSubject().toString().substring(0, 1).equals("\"")) || (triple.getSubject().toString().substring(0, 1).equals("'")) ? (triple.getSubject().toString().substring((triple.getSubject().toString().length() - 1), (triple.getSubject().toString().length())).equals("\"")) || (triple.getSubject().toString().substring((triple.getSubject().toString().length() - 1), (triple.getSubject().toString().length())).equals("'")) ? (triple.getSubject().toString().substring(1, (triple.getSubject().toString().length() - 1))) : triple.getSubject().toString() : triple.getSubject().toString();
            String predicate = triple.getPredicate().toString();
            String object = (triple.getObject().toString().substring(0, 1).equals("\"")) || (triple.getObject().toString().substring(0, 1).equals("'")) ? (triple.getObject().toString().substring((triple.getObject().toString().length() - 1), (triple.getObject().toString().length())).equals("\"")) || (triple.getObject().toString().substring((triple.getObject().toString().length() - 1), (triple.getObject().toString().length())).equals("'")) ? (triple.getObject().toString().substring(1, (triple.getObject().toString().length() - 1))) : triple.getObject().toString() : triple.getObject().toString();

            Long outputSize = getOutputSize(subject, predicate, object);
            TripleOperator to = TripleOperator.newTripleOperator(subject, predicate, object);

            Map<SparqlColumn, SparqlColumn> hm = new LinkedHashMap<>();

            for (SparqlColumn c : to.getArrayColumns()) {
                hm.put(c, c.copyToNewObject(Integer.toString(to.hashCode())));
            }

            RenameOperator p = RenameOperator.newRenameOperator(to, hm);

            List<OperandPair> k = new ArrayList<>();

            for (SparqlColumn c : p.getArrayColumns()) {
                if (!(c instanceof ColumnWithVariable)) {
                    k.add(OperandPair.newOperandPair(ColumnOperand.newColumnOperand(c), ValueOperand.newValueOperand(c.getQueryString()), ConditionType.EQ));
                }

            }

            listOfSelectOperators.add(SelectOperator.newSelectOperator(p, p.getArrayColumns(), k.stream().toArray(OperandPair[]::new), outputSize));

        });

        if (root == null) {
            root = formBaseOperator(formStarQueriesAndRemainingTriplets(/*checkForShortcuts(*/listOfSelectOperators/*)*/));
        } else {
            root = UnionOperator.newUnionOperator(root, formBaseOperator(formStarQueriesAndRemainingTriplets(/*checkForShortcuts(*/listOfSelectOperators/*)*/)));
        }
    }

//    private void getTriples(String q) {
//        Query query = QueryFactory.create(q);
//        System.out.println("THELIMIT: "+query.getProject().getVars().get(0).);
//
//        Op op = Algebra.compile(query);
//        this.myOpVisitorWalker(op);
//
//
//    }

    private List<BaseOperator> formStarQueriesAndRemainingTriplets(List<SelectOperator> listOfFilters) {

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
                JoinSubjectOperator orp = JoinSubjectOperator.newJoinOrOperator(aStarQueryTripletsList.stream().toArray(BaseOperator[]::new));
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

        //return a list with the JoinOrOperators and SelectOf Operators
        return starQueryTreeList;
    }

    private BaseOperator formBaseOperator(List<BaseOperator> l) {

        /* The method formBaseOperatorsWithCommonColumnPredicates forms Joins on given
        Bese Operators with common variables. The method formBaseOperator forms joins
        on given Base Operators that do not have common variables. The formBaseOperator
        method forms the whole tree using these kind of Joins.
        */
        List<BaseOperator> bopList = formBaseOperatorsWithCommonColumnPredicates(l);
        if (!optimized) {
            /*
            The list is reversed because it is read reversed. By reading reverse, we
            achieve that the deletion will happen on thelast element of the list
            which is efficient. It is used only in the non-optimized case since in
            the optimized case a sorting procedure takes place
             */
            Collections.reverse(bopList);
        }

        int initialBopListSize = bopList.size();

        while (initialBopListSize >= 2) {

            if (optimized) {
                bopList.sort((bo1, bo2) -> Long.compare(bo2.getOutputSize(), bo1.getOutputSize()));
            }

            JoinOperator joinOperator = JoinOperator.newJoinOperator(bopList.get(bopList.size() - 1), bopList.get(bopList.size() - 2));
            bopList.set(bopList.size() - 2, joinOperator);
            bopList.remove(bopList.size() - 1);

            initialBopListSize--;
        }

        return bopList.get(0);
    }

    private List<BaseOperator> formBaseOperatorsWithCommonColumnPredicates(List<BaseOperator> l) {

        if (optimized) {
            l.sort((bo1, bo2) -> Long.compare(bo1.getOutputSize(), bo2.getOutputSize()));
        }

        int i = 0;
        while (i < l.size()) {

            BaseOperator choosenBop = l.get(i);

            for (int j = i + 1; j < l.size(); j++) {

                if (choosenBop.hasCommonVariable(l.get(j))) {

                    l.set(i, JoinOperator.newJoinOperator(choosenBop, l.get(j)));
                    l.remove(j);

                    return formBaseOperatorsWithCommonColumnPredicates(l);
                }
            }
            i++;
        }

        return l;

    }

    private List<SelectOperator> checkForShortcuts(List<SelectOperator> l) {

        final Set<String> set = new HashSet<>(Arrays.asList(Consts.uriHasGeometry(), Consts.uriMBR(), Consts.uriHasTemporalFeature(), Consts.uriTimeStart()));

        Map<String[], String> hashMap = new HashMap<>();
        hashMap.put(new String[]{Consts.uriHasGeometry(), Consts.uriMBR()}, Consts.tripleMBRField());
        hashMap.put(new String[]{Consts.uriHasTemporalFeature(), Consts.uriTimeStart()}, Consts.tripleTimeStartField());

        //word - The list which contains the word as a predicate
        Map<String, List<SelectOperator>> p = l.stream().filter(f
                -> (!f.isPredicateVariable()) ? (set.contains(f.getPredicate()) && (f.isSubjectVariable() || f.isObjectVariable())) : false
        ).collect(Collectors.groupingBy(ba -> ba.getPredicate()));

        hashMap.forEach((k, v) -> {
            if (p.containsKey(k[0]) && p.containsKey(k[1])) {

                List<SelectOperator> list1 = p.get(k[0]);
                List<SelectOperator> list2 = p.get(k[1]);

                for (SelectOperator l1 : list1) {
                    int i = 0;
                    while (i < list2.size()) {
                        if (l1.getObject().equals(list2.get(i).getSubject())) {

                            TripleOperator to = TripleOperator.newTripleOperator(l1.getSubject(), v, list2.get(i).getObject());

                            Map<SparqlColumn, SparqlColumn> hm = new LinkedHashMap<>();

                            for (SparqlColumn c : to.getArrayColumns()) {
                                hm.put(c, c.copyToNewObject(Integer.toString(to.hashCode())));
                            }

                            RenameOperator pop = RenameOperator.newRenameOperator(to, hm);

                            List<OperandPair> cwv = new ArrayList<>();
                            for (SparqlColumn c : pop.getArrayColumns()) {
                                if (!(c instanceof ColumnWithVariable)) {
                                    cwv.add(OperandPair.newOperandPair(ColumnOperand.newColumnOperand(c), ValueOperand.newValueOperand(c.getQueryString()), ConditionType.EQ));
                                }
                            }

                            l.set(l.indexOf(l1), SelectOperator.newSelectOperator(pop, pop.getArrayColumns(), cwv.stream().toArray(OperandPair[]::new), new java.util.Random().nextInt(2000) + 1));

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

    public static class Builder {
        private final String sparqlQuery;

        private boolean optimized = false;

        private Builder(String sparqlQuery) {
            this.sparqlQuery = sparqlQuery;

        }

        public Builder optimized() {
            optimized = true;
            return this;
        }

        public LogicalPlanner build() {
            return new LogicalPlanner(this);
        }

    }

    private LogicalPlanner(Builder builder) {
        optimized = builder.optimized;
        //getTriples(builder.sparqlQuery);

        Query query = QueryFactory.create(builder.sparqlQuery);

        Op op = Algebra.compile(query);

        this.myOpVisitorWalker(op);


        if (filters != null) {

            for (Expr expr : filters) {
                ConditionType ct = null;

                BaseOperand bo1 = null;
                BaseOperand bo2 = null;

                String s = expr.toString().substring(1, expr.toString().length() - 1);

                String[] elements = s.split(" ");

                if(elements.length==1){
                    //if filter has single argument then pass it
                    if(elements[0].startsWith("?")){

                        for (SparqlColumn c : root.getArrayColumns()) {
                            if (c.getQueryString().equals(elements[0])) {
                                bo1 = ColumnOperand.newColumnOperand(c);
                                break;
                            }
                        }

                        root = SelectOperator.newSelectOperator(root, root.getArrayColumns(),  new BaseOperand[]{bo1}, root.getOutputSize());
                    }
                    else{
                        root = SelectOperator.newSelectOperator(root, root.getArrayColumns(), new BaseOperand[]{ValueOperand.newValueOperand(elements[0])}, root.getOutputSize());
                    }
                    continue;
                }

                switch (elements[0]) {
                    case "=":
                        ct = ConditionType.EQ;
                        break;
                    case "<":
                        ct = ConditionType.LT;
                        break;
                    case ">":
                        ct = ConditionType.GT;
                        break;
                    case "<=":
                        ct = ConditionType.LTE;
                        break;
                    case ">=":
                        ct = ConditionType.GTE;
                        break;
                }


                if (elements[1].startsWith("?")) {
                    for (SparqlColumn c : root.getArrayColumns()) {
                        if (c.getQueryString().equals(elements[1])) {
                            bo1 = ColumnOperand.newColumnOperand(c);
                            break;
                        }
                    }
                } else {
                    bo1 = ValueOperand.newValueOperand(elements[1]);
                }

                if (elements[2].startsWith("?")) {
                    for (SparqlColumn c : root.getArrayColumns()) {
                        if (c.getQueryString().equals(elements[2])) {
                            bo2 = ColumnOperand.newColumnOperand(c);
                            break;
                        }
                    }
                } else {
                    bo2 = ValueOperand.newValueOperand(elements[2]);
                }

                if (bo1 == null || bo2 == null) {
                    try {
                        throw new Exception("Filter's Variable was not found in WHERE clause");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                root = SelectOperator.newSelectOperator(root, root.getArrayColumns(), new OperandPair[]{OperandPair.newOperandPair(bo1, bo2, ct)}, root.getOutputSize());

            }

        }

        if (query.isDistinct()) {

            root = DistinctOperator.newDistinctOperator(root);

        }


        if (query.hasOrderBy()) {
            List<ColumnWithDirection> cwd = new ArrayList<>();

            for (SortCondition sc : query.getOrderBy()) {

                for (SparqlColumn c : root.getArrayColumns()) {
                    if (c instanceof ColumnWithVariable) {
                        if (c.getQueryString().equals(sc.expression.getExprVar().toString())) {
                            int directionInt = sc.direction;
                            if (sc.direction == -2) {
                                directionInt = 1;
                            }
                            SortDirection direction = SortDirection.ASC;
                            if (directionInt < 0) {
                                direction = SortDirection.DESC;
                            }
                            cwd.add(ColumnWithDirection.newColumnWithDirection(c, direction));
                            break;
                        }
                    }
                }
            }

            if (cwd.size() != query.getOrderBy().size()) {
                try {
                    throw new Exception("Order by variable was not found in Array Columns");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            root = SortOperator.newSortOperator(root, cwd.stream().toArray(ColumnWithDirection[]::new));


        }

        if (query.hasLimit()) {
            root = LimitOperator.newLimitOperator(root, (int) query.getLimit());
        }

        root = ProjectOperator.newProjectOperator(root, selectVariables);

    }

    public static Builder setSparqlQuery(String sparqlQuery) {
        return new Builder(sparqlQuery);
    }

}
