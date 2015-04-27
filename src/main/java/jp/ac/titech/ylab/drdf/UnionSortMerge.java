package jp.ac.titech.ylab.drdf;

import java.sql.*;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.*;

class Slave1a implements Callable<HashSet<String>> {
    String q = null;
    Connection c11 = null;
    PreparedStatement pst11 = null;
    static HashSet<String> joinResult1a = new HashSet<String>();

    public Slave1a(String query, Connection c1, PreparedStatement pst1) {
        // TODO Auto-generated constructor stub
        q = query;

        c11 = c1;
        pst11 = pst1;
    }

    public HashSet<String> call() throws SQLException {

        ResultSet rs1 = null;

        pst11 = c11.prepareStatement(q);
        rs1 = pst11.executeQuery();

        while (rs1.next()) {
            joinResult1a.add(rs1.getString("pred"));
        }
        return (joinResult1a);

    }


}

class Slave1b implements Callable<HashSet<String>> {
    String q_2 = null;
    Connection c1_2 = null;
    PreparedStatement pst1_2 = null;
    static HashSet<String> joinResult1b = new HashSet<String>();

    public Slave1b(String query2, Connection c1, PreparedStatement pst1) {
        // TODO Auto-generated constructor stub
        q_2 = query2;

        c1_2 = c1;
        pst1_2 = pst1;
    }

    public HashSet<String> call() throws SQLException {

        ResultSet rs1_2 = null;

        pst1_2 = c1_2.prepareStatement(q_2);
        rs1_2 = pst1_2.executeQuery();

        while (rs1_2.next()) {
            joinResult1b.add(rs1_2.getString("pred"));
        }
        return (joinResult1b);

    }


}

class Slave2a implements Callable<HashSet<String>> {
    String q2 = null;
    Connection c22 = null;
    PreparedStatement pst22 = null;
    static HashSet<String> joinResult2a = new HashSet<String>();

    public Slave2a(String query, Connection c2, PreparedStatement pst2) {
        // TODO Auto-generated constructor stub
        q2 = query;
        c22 = c2;
        pst22 = pst2;
    }

    public HashSet<String> call() throws SQLException {

        ResultSet rs2 = null;

        pst22 = c22.prepareStatement(q2);
        rs2 = pst22.executeQuery();
        while (rs2.next()) {
            joinResult2a.add(rs2.getString("pred"));
        }
        return (joinResult2a);

    }


}

class Slave2b implements Callable<HashSet<String>> {
    String q2_2 = null;
    Connection c2_2 = null;
    PreparedStatement pst2_2 = null;
    HashSet<String> joinResult2b = new HashSet<String>();

    public Slave2b(String query2, Connection c2, PreparedStatement pst2) {
        // TODO Auto-generated constructor stub
        q2_2 = query2;

        c2_2 = c2;
        pst2_2 = pst2;
    }

    public HashSet<String> call() throws SQLException {

        ResultSet rs2_2 = null;

        pst2_2 = c2_2.prepareStatement(q2_2);
        rs2_2 = pst2_2.executeQuery();

        while (rs2_2.next()) {
            joinResult2b.add(rs2_2.getString("pred"));
        }
        return (joinResult2b);

    }


}

class Slave3a implements Callable<HashSet<String>> {
    String q3 = null;
    Connection c33 = null;
    PreparedStatement pst33 = null;
    static HashSet<String> joinResult3a = new HashSet<String>();

    public Slave3a(String query, Connection c3, PreparedStatement pst3) {
        // TODO Auto-generated constructor stub
        q3 = query;
        c33 = c3;
        pst33 = pst3;
    }

    public HashSet<String> call() throws SQLException {
        ResultSet rs3 = null;
        pst33 = c33.prepareStatement(q3);
        rs3 = pst33.executeQuery();
        while (rs3.next()) {
            joinResult3a.add(rs3.getString("pred"));
        }
        return (joinResult3a);

    }


}

class Slave3b implements Callable<HashSet<String>> {
    String q3_3 = null;
    Connection c3_3 = null;
    PreparedStatement pst3_3 = null;
    HashSet<String> joinResult3b = new HashSet<String>();

    public Slave3b(String query2, Connection c3, PreparedStatement pst3) {
        // TODO Auto-generated constructor stub
        q3_3 = query2;
        c3_3 = c3;
        pst3_3 = pst3;
    }

    public HashSet<String> call() throws SQLException {

        ResultSet rs3_3 = null;

        pst3_3 = c3_3.prepareStatement(q3_3);
        rs3_3 = pst3_3.executeQuery();

        while (rs3_3.next()) {
            joinResult3b.add(rs3_3.getString("pred"));
        }
        return (joinResult3b);

    }


}

class Slave4a implements Callable<HashSet<String>> {
    String q4 = null;
    Connection c44 = null;
    PreparedStatement pst44 = null;
    static HashSet<String> joinResult4a = new HashSet<String>();

    public Slave4a(String query, Connection c4, PreparedStatement pst4) {
        // TODO Auto-generated constructor stub
        q4 = query;
        c44 = c4;
        pst44 = pst4;
    }

    public HashSet<String> call() throws SQLException {
        ResultSet rs4 = null;
        pst44 = c44.prepareStatement(q4);
        rs4 = pst44.executeQuery();
        while (rs4.next()) {
            joinResult4a.add(rs4.getString("pred"));
        }
        return (joinResult4a);

    }


}

class Slave4b implements Callable<HashSet<String>> {
    String q4_4 = null;
    Connection c4_4 = null;
    PreparedStatement pst4_4 = null;
    static HashSet<String> joinResult4b = new HashSet<String>();

    public Slave4b(String query2, Connection c4, PreparedStatement pst4) {
        // TODO Auto-generated constructor stub
        q4_4 = query2;
        c4_4 = c4;
        pst4_4 = pst4;
    }

    public HashSet<String> call() throws SQLException {
        ResultSet rs4_4 = null;
        pst4_4 = c4_4.prepareStatement(q4_4);
        rs4_4 = pst4_4.executeQuery();
        while (rs4_4.next()) {
            joinResult4b.add(rs4_4.getString("pred"));
        }
        return (joinResult4b);

    }


}


public class UnionSortMerge {

    public static void main(String[] args) throws ClassNotFoundException, InterruptedException, SQLException {
        final String query = "SELECT DISTINCT b.predicate pred"
                + " from RDFHASHEDBYSUBJECT a, RDFHASHEDBYSUBJECT b"
                + " WHERE a.subject = b.subject"
                + " and a.predicate='rdf:type' and a.object='foaf:Person';";
        final String query2 = "SELECT DISTINCT b.predicate pred from RDFHASHEDBYSUBJECT a, RDFHASHEDBYOBJECT b WHERE"
                + " a.subject = b.object and a.predicate='rdf:type' and a.object='foaf:Person';";
        Class.forName("org.postgresql.Driver");
        HashSet<String> set = new HashSet<String>();

        String result = "";
        PreparedStatement pst1 = null;
        Connection c1 = DriverManager
                .getConnection(
                        "jdbc:postgresql://192.168.172.174:5432/server1", "postgres",
                        "root");
        PreparedStatement pst2 = null;
        Connection c2 = DriverManager
                .getConnection(
                        "jdbc:postgresql://192.168.172.175:5432/server2", "postgres",
                        "root");
        PreparedStatement pst3 = null;
        Connection c3 = DriverManager
                .getConnection(
                        "jdbc:postgresql://192.168.172.176:5432/server3", "postgres",
                        "root");
        PreparedStatement pst4 = null;
        Connection c4 = DriverManager
                .getConnection(
                        "jdbc:postgresql://192.168.172.177:5432/server4", "postgres",
                        "root");

        final ExecutorService service;
        final Future<HashSet<String>> task;
        final Future<HashSet<String>> task1;
        final Future<HashSet<String>> task2;
        final Future<HashSet<String>> task3;
        final Future<HashSet<String>> task4;
        final Future<HashSet<String>> task5;
        final Future<HashSet<String>> task6;
        final Future<HashSet<String>> task7;
        service = Executors.newFixedThreadPool(8);
        task = service.submit(new Slave1a(query, c1, pst1));
        task1 = service.submit(new Slave2a(query, c2, pst2));
        task2 = service.submit(new Slave3a(query, c3, pst3));
        task3 = service.submit(new Slave4a(query, c4, pst4));
        task4 = service.submit(new Slave1b(query2, c1, pst1));
        task5 = service.submit(new Slave2b(query2, c2, pst2));
        task6 = service.submit(new Slave3b(query2, c3, pst3));
        task7 = service.submit(new Slave4b(query2, c4, pst4));
        try {
            final HashSet<String> r1;
            final HashSet<String> r2;
            final HashSet<String> r3;
            final HashSet<String> r4;
            final HashSet<String> r11;
            final HashSet<String> r12;
            final HashSet<String> r13;
            final HashSet<String> r14;

            // waits the 10 seconds for the Callable.call to finish.

            r1 = task.get();
            r2 = task1.get();
            r3 = task2.get();
            r4 = task3.get();
            r11 = task4.get();
            r12 = task5.get();
            r13 = task6.get();
            r14 = task7.get();
            set.addAll(r1);
            set.addAll(r2);
            set.addAll(r3);
            set.addAll(r4);
            set.addAll(r11);
            set.addAll(r12);
            set.addAll(r13);
            set.addAll(r14);
            Iterator<String> litr = set.iterator();
            while (litr.hasNext()) {
                Object element = litr.next();
                System.out.println(element);
            }
            // System.out.println(str1);
        } catch (final ExecutionException ex) {
            ex.printStackTrace();
        }
        service.shutdownNow();
    }

}
