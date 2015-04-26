package jp.ac.titech.ylab.drdf;


import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementVisitorBase;
import com.hp.hpl.jena.sparql.syntax.ElementWalker;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.sql.ResultSet;
import java.util.*;
import java.util.Map.Entry;

public class QueryProcessingAlgorithm {
    @Option(name = "-h")
    private boolean showHelp = false;

    @Option(name = "-q", usage = "query", required = false)
    private String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
            + "PREFIX dc: <http://purl.org/dc/elements/1.1/>"
            + "PREFIX bench:   <http://localhost/vocabulary/bench/>"
            + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
            + "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "

            + "select distinct ?name ?person" + " where {"
            + "?article rdf:type bench:Article ."
            + "?article dc:creator ?person ."
            + "?inproc rdf:type bench:Inproceedings ."
            + "?inproc dc:creator ?person ."
            + "?person foaf:name ?name .}";


    static ArrayList<String> listOfTriples = new ArrayList<String>();
    static ArrayList<String> listOfSub = new ArrayList<String>();
    static ArrayList<String> listOfObj = new ArrayList<String>();
    static ArrayList<String> listOfPred = new ArrayList<String>();
    static ArrayList<String> tuple = new ArrayList<String>();
    static ArrayList<String> tupletemp = new ArrayList<String>();
    static ArrayList<String> Alltuple = new ArrayList<String>();
    static ArrayList<String> QueryParts = new ArrayList<String>();
    static ArrayList<String> Queryglobal = new ArrayList<String>();
    static ArrayList<String> Projection = new ArrayList<String>();
    static ArrayList<String> Projection1 = new ArrayList<String>();
    HashMap<String, String> ProjVariable = new HashMap<String, String>();
    static String Var1 = "";
    static String Var = "";
    static String projVar2 = "";
    static Set<String> tuplenew = new HashSet<String>();
    static int numberOfVar = 0;
    static String[] splitted = null;
    static String queryjoin = "";
    static String where = "";
    static String triple = "";
    static String subject = "";
    static String predicate = "";
    static String object = "";
    static String queryPart1 = "";
    static String projVar = "";
    static String tempWhere = "";
    static int countOfTriples = 0;
    static String queryCase1 = "";
    static int num = 0;
    static String CompleteQuery = "";
    static ResultSet rs1, rs2, rs3, rs4, rs5, rs6, rs7, rs8, rs9, rs10, rs11, rs12;
    static String k = "";
    static String temp = "";
    static String select = "";
    static Set<String> proj = new HashSet<String>();
    static Set<String> obj = new HashSet<String>();
    static Set<String> sub = new HashSet<String>();
    static Set<String> pred = new HashSet<String>();

    public static void main(String[] args) throws Exception {

        QueryProcessingAlgorithm app = new QueryProcessingAlgorithm();
        CmdLineParser parser = new CmdLineParser(app);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        if (app.showHelp) {
            parser.printUsage(System.err);
            System.exit(0);
        }
        app.run();
        System.exit(0);
    }

    static Set<String> findDuplicates(ArrayList<String> listContainingDuplicates) {
        final Set<String> setToReturn = new HashSet<String>();
        final Set<String> set1 = new HashSet<String>();
        for (String yourInt : listContainingDuplicates) {
            if (!set1.add(yourInt)) {
                setToReturn.add(yourInt);
            }
        }
        return setToReturn;
    }

    public void run() throws Exception {

        Query query = QueryFactory.create(queryString);
        HashMap<String, String> hmap = new HashMap<String, String>();
        hmap.put("http://purl.org/dc/elements/1.1/", "dc:");
        hmap.put("http://purl.org/dc/terms/", "dcterms:");
        hmap.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:");
        hmap.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");
        hmap.put("http://swrc.ontoware.org/ontology#", "swrc:");
        hmap.put("http://xmlns.com/foaf/0.1/", "foaf:");
        hmap.put("http://localhost/vocabulary/bench/", "bench:");
        hmap.put("http://www.w3.org/2001/XMLSchema#", "xsd:");
        hmap.put("http://localhost/persons/", "person:");

        Query query3 = QueryFactory.create(queryString);
        Op op = Algebra.compile(query3);
        System.out.println(op);
        String p = "project";
        int h = op.toString().indexOf("project") + p.length() + 2;
        String[] words = op.toString().substring(h).split("\\)+");
        String[] g = words[0].split("\\s");
        for (String i : g) {
            //System.out.println(i);
            Projection.add(i.trim());
            //a.add(w.split("\\s"));
        }
        System.out.println("Projection" + Projection);
        ElementWalker.walk(query.getQueryPattern(),
                // For each element...
                new ElementVisitorBase() {

                    // ...when it's a block of triples...
                    public void visit(ElementPathBlock el) {
                        // ...go through all the triples...
                        Iterator<TriplePath> triples = el.patternElts();
                        while (triples.hasNext()) {
                            TriplePath t = triples.next();
                            triple = t.toString();
                            Alltuple.add(triple);
                            object = t.getObject().toString();
                            subject = t.getSubject().toString();
                            predicate = t.getPredicate().toString();
                            listOfTriples.add(triple);
                            listOfSub.add(subject);
                            listOfObj.add(object);
                            listOfPred.add(predicate);
                            if (object.substring(0, 1).equals("?")) {
                                tuple.add(object);
                                obj.add(object);
                            }
                            if (subject.substring(0, 1).equals("?")) {
                                tuple.add(subject);
                                sub.add(subject);
                            }
                            if (predicate.substring(0, 1).equals("?")) {
                                tuple.add(predicate);
                                pred.add(predicate);
                            }
                        }

                    }
                });

        countOfTriples = listOfTriples.size();
        System.out.println("number of triples " + countOfTriples);
        //case 1 with only one triple
        if (countOfTriples == 1) {

            if (obj.isEmpty() && !pred.isEmpty())
                queryCase1 = " from RDFHashedByObject where object='" + object + "';";
            else if (obj.isEmpty() && pred.isEmpty())
                queryCase1 = " from RDFHashedByObject where object='" + object + "' and predicate='" + predicate + "';";
            else if (sub.isEmpty() && !pred.isEmpty())
                queryCase1 = " from RDFHashedBySubject where subject='" + subject + "';";
            else
                queryCase1 = " from RDFHashedBySubject where subject='" + subject + "' and predicate='" + predicate + "';";
            for (int i = 0; i < Projection.size(); i++) {
                if (Projection.get(i).equals(subject))
                    select = select + "T0.subject";
                if (Projection.get(i).equals(predicate))
                    select = select + "T0.predicate";
                if (Projection.get(i).equals(object))
                    select = select + "T0.object";
            }
            CompleteQuery = "Select " + select + queryCase1;
        }
        //case 2 with 2 triples
        else if (countOfTriples == 2) {
            System.out.println(Alltuple);
            for (int i = 0; i < countOfTriples; i++) {
                for (int j = i + 1; j < countOfTriples; j++) {
                    if (listOfSub.get(i).substring(0, 1).equals("?") && listOfSub.get(j).substring(0, 1).equals("?") && listOfSub.get(i).equals(listOfSub.get(j)))
                        queryCase1 = " from RDFHashedBySubject T" + i + ",RDFHashedBySubject T" + j + " where T" + i + ".subject = T" + j + ".subject";
                    if (listOfObj.get(i).substring(0, 1).equals("?") && listOfSub.get(j).substring(0, 1).equals("?") && listOfObj.get(i).equals(listOfSub.get(j)))
                        queryCase1 = " from RDFHashedByObject T" + i + ",RDFHashedBySubject T" + j + " where T" + i + ".object = T" + j + ".subject";

                    if (listOfSub.get(i).substring(0, 1).equals("?") && listOfObj.get(j).substring(0, 1).equals("?") && listOfSub.get(i).equals(listOfObj.get(j)))
                        queryCase1 = " from RDFHashedBySubject T" + i + ",RDFHashedByObject T" + j + " where T" + i + ".subject = T" + j + ".object";

                    if (listOfObj.get(i).substring(0, 1).equals("?") && listOfObj.get(j).substring(0, 1).equals("?") && listOfObj.get(i).equals(listOfObj.get(j)))
                        queryCase1 = " from RDFHashedByObject T" + i + ",RDFHashedByObject T" + j + " where T" + i + ".object = T" + j + ".object";

                }
            }
            for (int i = 0; i < countOfTriples; i++) {
                if (!listOfPred.get(i).substring(0, 1).equals("?")) {
                    queryCase1 = queryCase1 + " and T" + i + ".predicate = '" + listOfPred.get(i) + "'";
                }
                if (!listOfObj.get(i).substring(0, 1).equals("?")) {
                    queryCase1 = queryCase1 + " and T" + i + ".object = '" + listOfObj.get(i) + "'";
                }
            }
            for (int x = 0; x < Projection.size(); x++) {
                for (int i = 0; i < Alltuple.size(); i++) {

                    int q = Alltuple.get(i).indexOf(" ");
                    String subjecttemp = Alltuple.get(i).substring(0, q);
                    String predicateandobjtemp = Alltuple.get(i).substring(q + 1);
                    int q1 = predicateandobjtemp.indexOf(" ");
                    String predicatetemp = predicateandobjtemp.substring(0, q1);
                    String objecttemp = predicateandobjtemp.substring(q1 + 1);
                    if (subjecttemp.equals(Projection.get(x))) {

                        if (ProjVariable.containsKey(subjecttemp)) {
                            ProjVariable.put(subjecttemp, "T" + i + ".subject");
                        } else
                            ProjVariable.put(subjecttemp, "T" + i + ".subject");

                    }
                    if (predicatetemp.equals(Projection.get(x))) {

                        if (ProjVariable.containsKey(predicatetemp)) {
                            ProjVariable.put(predicatetemp, "T" + i + ".predicate");
                        } else
                            ProjVariable.put(predicatetemp, "T" + i + ".predicate");

                    }
                    if (objecttemp.equals(Projection.get(x))) {

                        if (ProjVariable.containsKey(objecttemp)) {
                            ProjVariable.put(objecttemp, "T" + i + ".object");
                        } else
                            ProjVariable.put(objecttemp, "T" + i + ".object");

                    }
                }
            }
            Set<Entry<String, String>> set = ProjVariable.entrySet();
            Iterator<Entry<String, String>> j1 = set.iterator();
            while (j1.hasNext()) {
                Map.Entry me = (Map.Entry) j1.next();
                System.out.println(me.getValue());
                projVar2 = projVar2 + me.getValue() + " " + me.getKey().toString().substring(1) + ",";
            }

            projVar = "Select " + projVar2.substring(0, projVar2.length() - 1);

            CompleteQuery = projVar + queryCase1;


        }
        //case 3 if number of triples are greater than 2
        else {

            tuplenew = findDuplicates(tuple);
            for (String s : tuplenew) //for each distinct repeating variable
            {
                numberOfVar += 1;
                for (int j = 0; j < Alltuple.size(); j++) {
                    if (Alltuple.get(j).contains(s))
                        tupletemp.add(Alltuple.get(j));
                }
                // Query1

                for (int i = 0; i < tupletemp.size(); i++) {
                    for (int j = 0; j < Projection.size(); j++)
                        if (tupletemp.get(i).contains(Projection.get(j))) {
                            proj.add(Projection.get(j));
                            num = i;
                        }
                }
                for (int i = 0; i < tupletemp.size(); i++) {
                    if (proj.size() == Projection.size()) {
                        Var1 = Var1 + "T" + i + ".tripleId T" + i + ",";
                    }
                }
                System.out.println("projection is" + Projection);
                if (proj.size() == Projection.size()) {
                    //System.out.println("enter");
                    for (int x = 0; x < Projection.size(); x++) {
                        for (int i = 0; i < tupletemp.size(); i++) {
                            int q = tupletemp.get(i).indexOf(" ");
                            String subjecttemp = tupletemp.get(i).substring(0, q);
                            String predicateandobjtemp = tupletemp.get(i).substring(q + 1);
                            int q1 = predicateandobjtemp.indexOf(" ");
                            String predicatetemp = predicateandobjtemp.substring(0, q1);
                            String objecttemp = predicateandobjtemp.substring(q1 + 1);
                            if (subjecttemp.equals(Projection.get(x))) {

                                if (ProjVariable.containsKey(subjecttemp)) {
                                    ProjVariable.put(subjecttemp, "T" + i + ".subject");
                                } else
                                    ProjVariable.put(subjecttemp, "T" + i + ".subject");

                            }
                            if (predicatetemp.equals(Projection.get(x))) {

                                if (ProjVariable.containsKey(predicatetemp)) {
                                    ProjVariable.put(predicatetemp, "T" + i + ".predicate");
                                } else
                                    ProjVariable.put(predicatetemp, "T" + i + ".predicate");

                            }
                            if (objecttemp.equals(Projection.get(x))) {
                                if (ProjVariable.containsKey(objecttemp)) {
                                    ProjVariable.put(objecttemp, "T" + i + "object");
                                } else
                                    ProjVariable.put(objecttemp, "T" + i + ".object");

                            }


                        }

                    }
                    Set set = ProjVariable.entrySet();
                    Iterator j1 = set.iterator();
                    while (j1.hasNext()) {
                        Map.Entry me = (Map.Entry) j1.next();
                        System.out.println(me.getValue());
                        projVar2 = projVar2 + me.getValue() + " " + me.getKey().toString().substring(1) + ",";
                    }

                    projVar = "Select " + Var1 + projVar2.substring(0, projVar2.length() - 1);
                } else
                    projVar = "select T" + num + ".tripleId T" + numberOfVar;

                for (int i = 0; i < tupletemp.size(); i++) {
                    int k = tupletemp.get(i).indexOf(" ");
                    String subject = tupletemp.get(i).substring(0, k);
                    String predicateandobj = tupletemp.get(i).substring(k + 1);
                    int k1 = predicateandobj.indexOf(" ");
                    String predicate = predicateandobj.substring(0, k1);
                    String ob = predicateandobj.substring(k1 + 1);
                    String object = ob.substring(0, ob.length());

                    for (int j = i + 1; j < tupletemp.size(); j++) {
                        int k2 = tupletemp.get(j).indexOf(" ");
                        String subject1 = tupletemp.get(j).substring(0, k2);
                        String predicateandobj1 = tupletemp.get(j).substring(k2 + 1);
                        int k11 = predicateandobj1.indexOf(" ");
                        String predicate1 = predicateandobj1.substring(0, k11);
                        String ob1 = predicateandobj1.substring(k11 + 1);
                        String object1 = ob1.substring(0, ob1.length());
                        if (!subject.substring(0, 1).equals("?")) {
                            tempWhere = " T" + i + ".subject='" + subject + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (!predicate.substring(0, 1).equals("?")) {
                            tempWhere = " T" + i + ".predicate='" + predicate + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (!object.substring(0, 1).equals("?")) {
                            tempWhere = " T" + i + ".object='" + object + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (!subject1.substring(0, 1).equals("?")) {
                            tempWhere = " T" + j + ".subject='" + subject1 + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (!predicate1.substring(0, 1).equals("?")) {
                            tempWhere = " T" + j + ".predicate='" + predicate1 + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (!object1.substring(0, 1).equals("?")) {
                            tempWhere = " T" + j + ".object='" + object1 + "' and";
                            if (!where.contains(tempWhere))
                                where = where + tempWhere;
                        }
                        if (subject.equals(subject1)) {
                            queryPart1 = " from RDFHashedBySubject T0";
                            queryjoin = queryjoin + " join RDFHashedBySubject T" + j + " on T" + i + ".subject= T" + j + ".subject";
                            i++;
                        } else if (subject.equals(object1)) {
                            queryPart1 = " from RDFHashedBySubject T0";
                            queryjoin = queryjoin + " join RDFHashedByObject T" + j + " on T" + i + ".subject= T" + j + ".object";
                            i++;
                        } else if (object.equals(subject1)) {
                            queryPart1 = " from RDFHashedByObject T0";
                            queryjoin = queryjoin + " join RDFHashedBySubject T" + j + " on T" + i + ".object= T" + j + ".subject";
                            i++;
                        } else {
                            queryPart1 = " from RDFHashedByObject T0";
                            queryjoin = queryjoin + " join RDFHashedByObject T" + j + " on T" + i + ".object= T" + j + ".object";
                            i++;
                        }
                    }

                }
                tupletemp.clear();
                for (Map.Entry<String, String> hEntry : hmap.entrySet()) {
                    String prefixKey = hEntry.getKey().toString();
                    String prefixValue = hEntry.getValue().toString();
                    if (queryjoin.contains(prefixKey)) {
                        queryjoin = queryjoin.replaceAll(prefixKey, prefixValue);
                    }
                    if (where.contains(prefixKey)) {
                        where = where.replaceAll(prefixKey, prefixValue);
                    }
                }
                String whereClause = where.replaceAll("[(<>)]", "");
                //System.out.println("where clause is"+whereClause);
                System.out.println("projvar is" + projVar);
                String completeQuery = projVar + queryPart1 + queryjoin + " and" + whereClause.substring(0, whereClause.length() - 3) + ";";
                System.out.println("complete query is" + completeQuery);
                QueryParts.add(completeQuery);
                queryjoin = "";
                where = "";

            }

            System.out.println(QueryParts);
            System.out.println(QueryParts.get(2));


        }
        for (Map.Entry<String, String> hEntry : hmap.entrySet()) {
            String prefixKey = hEntry.getKey().toString();
            String prefixValue = hEntry.getValue().toString();
            if (CompleteQuery.contains(prefixKey)) {
                CompleteQuery = CompleteQuery.replaceAll(prefixKey, prefixValue);
            }
            if (CompleteQuery.contains(prefixKey)) {
                CompleteQuery = CompleteQuery.replaceAll(prefixKey, prefixValue);
            }
        }
        //System.out.println("CompleteQuery);
    }
}
