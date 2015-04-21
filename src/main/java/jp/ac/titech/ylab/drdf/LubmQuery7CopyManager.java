package jp.ac.titech.ylab.drdf;

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class LubmQuery7CopyManager {
  String q1p1 =
      "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate="
          + " 'ub:takesCourse' and abs(T2.tripleid%4)=0";
  String q1p2 =
      "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate="
          + " 'ub:takesCourse' and abs(T2.tripleid%4)=1";
  String q1p3 =
      "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate="
          + " 'ub:takesCourse' and abs(T2.tripleid%4)=2";
  String q1p4 =
      "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate="
          + " 'ub:takesCourse' and abs(T2.tripleid%4)=3";
  String q2p1 =
      "SELECT T2.tripleid triple1,T1.subject as course,T2.subject as student from rdfhashedbysubject T1,rdfhashedbyobject T2,rdfhashedbyobject T3 where T1.subject=T2.object and T2.object=T3.object"
          + " and T1.predicate='rdf:type' and T1.object='<ub:Course>' and T2.predicate='ub:takesCourse' and T3.subject='<http://www.Department0.University0.edu/AssociateProfessor0>' and T3.predicate='ub:teacherOf' and abs(T2.tripleid%4)=0";
  String q2p2 =
      "SELECT T2.tripleid triple1,T1.subject as course,T2.subject as student from rdfhashedbysubject T1,rdfhashedbyobject T2,rdfhashedbyobject T3 where T1.subject=T2.object and T2.object=T3.object"
          + " and T1.predicate='rdf:type' and T1.object='<ub:Course>' and T2.predicate='ub:takesCourse' and T3.subject='<http://www.Department0.University0.edu/AssociateProfessor0>' and T3.predicate='ub:teacherOf' and abs(T2.tripleid%4)=1";
  String q2p3 =
      "SELECT T2.tripleid triple1,T1.subject as course,T2.subject as student from rdfhashedbysubject T1,rdfhashedbyobject T2,rdfhashedbyobject T3 where T1.subject=T2.object and T2.object=T3.object"
          + " and T1.predicate='rdf:type' and T1.object='<ub:Course>' and T2.predicate='ub:takesCourse' and T3.subject='<http://www.Department0.University0.edu/AssociateProfessor0>' and T3.predicate='ub:teacherOf' and abs(T2.tripleid%4)=2";
  String q2p4 =
      "SELECT T2.tripleid triple1,T1.subject as course,T2.subject as student from rdfhashedbysubject T1,rdfhashedbyobject T2,rdfhashedbyobject T3 where T1.subject=T2.object and T2.object=T3.object"
          + " and T1.predicate='rdf:type' and T1.object='<ub:Course>' and T2.predicate='ub:takesCourse' and T3.subject='<http://www.Department0.University0.edu/AssociateProfessor0>' and T3.predicate='ub:teacherOf' and abs(T2.tripleid%4)=3";

  public static void main(String args[]) throws Exception {
    LubmQuery7CopyManager app = new LubmQuery7CopyManager();
    Connection c =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.174:5432/test", "postgres",
            "root");
    Connection c2 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.175:5432/test", "postgres",
            "root");
    Connection c3 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.176:5432/test", "postgres",
            "root");
    Connection c4 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.177:5432/test", "postgres",
            "root");
    Connection ct1 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.174:5432/test2", "postgres",
            "root");
    Connection ct2 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.175:5432/test2", "postgres",
            "root");
    Connection ct3 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.176:5432/test2", "postgres",
            "root");
    Connection ct4 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.177:5432/test2", "postgres",
            "root");

    Connection con1 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.174:5432/server1_lubm50",
            "postgres", "root");
    Connection con2 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.175:5432/server2_lubm50",
            "postgres", "root");
    Connection con3 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.176:5432/server3_lubm50",
            "postgres", "root");
    Connection con4 =
        DriverManager.getConnection("jdbc:postgresql://192.168.172.177:5432/server4_lubm50",
            "postgres", "root");



    // app.createTable(ct1,ct2,ct3,ct4);
    // app.createTable(c2);
    app.copy(c, c2, c3, c4, ct1, ct2, ct3, ct4, con1, con2, con3, con4);
  }

  public void createTable(Connection c, Connection c2, Connection c3, Connection c4)
      throws Exception {
    Statement stmt = c.createStatement();
    stmt.execute("create table test2(tripleid integer,course varchar(100),student varchar(100));");
    Statement stmt2 = c2.createStatement();
    stmt2.execute("create table test2(tripleid integer,course varchar(100),student varchar(100));");
    Statement stmt3 = c3.createStatement();
    stmt3.execute("create table test2(tripleid integer,course varchar(100),student varchar(100));");
    Statement stmt4 = c4.createStatement();
    stmt4.execute("create table test2(tripleid integer,course varchar(100),student varchar(100));");
  }

  public void copy(Connection c, Connection c2, Connection c3, Connection c4, Connection ct1,
      Connection ct2, Connection ct3, Connection ct4, Connection con1, Connection con2,
      Connection con3, Connection con4) throws Exception {
    PipedWriter writer1 = new PipedWriter();
    PipedWriter writer2 = new PipedWriter();
    PipedWriter writer3 = new PipedWriter();
    PipedWriter writer4 = new PipedWriter();
    PipedWriter writer5 = new PipedWriter();
    PipedWriter writer6 = new PipedWriter();
    PipedWriter writer7 = new PipedWriter();
    PipedWriter writer8 = new PipedWriter();
    PipedWriter writer9 = new PipedWriter();
    PipedWriter writer10 = new PipedWriter();
    PipedWriter writer11 = new PipedWriter();
    PipedWriter writer12 = new PipedWriter();
    PipedWriter writer13 = new PipedWriter();
    PipedWriter writer14 = new PipedWriter();
    PipedWriter writer15 = new PipedWriter();
    PipedWriter writer16 = new PipedWriter();
    // part2
    PipedWriter writer17 = new PipedWriter();
    PipedWriter writer18 = new PipedWriter();
    PipedWriter writer19 = new PipedWriter();
    PipedWriter writer20 = new PipedWriter();
    PipedWriter writer21 = new PipedWriter();
    PipedWriter writer22 = new PipedWriter();
    PipedWriter writer23 = new PipedWriter();
    PipedWriter writer24 = new PipedWriter();
    PipedWriter writer25 = new PipedWriter();
    PipedWriter writer26 = new PipedWriter();
    PipedWriter writer27 = new PipedWriter();
    PipedWriter writer28 = new PipedWriter();
    PipedWriter writer29 = new PipedWriter();
    PipedWriter writer30 = new PipedWriter();
    PipedWriter writer31 = new PipedWriter();
    PipedWriter writer32 = new PipedWriter();



    PipedReader reader1 = new PipedReader(writer1);
    PipedReader reader2 = new PipedReader(writer2);
    PipedReader reader3 = new PipedReader(writer3);
    PipedReader reader4 = new PipedReader(writer4);

    PipedReader reader5 = new PipedReader(writer5);
    PipedReader reader6 = new PipedReader(writer6);
    PipedReader reader7 = new PipedReader(writer7);
    PipedReader reader8 = new PipedReader(writer8);

    PipedReader reader9 = new PipedReader(writer9);
    PipedReader reader10 = new PipedReader(writer10);
    PipedReader reader11 = new PipedReader(writer11);
    PipedReader reader12 = new PipedReader(writer12);

    PipedReader reader13 = new PipedReader(writer13);
    PipedReader reader14 = new PipedReader(writer14);
    PipedReader reader15 = new PipedReader(writer15);
    PipedReader reader16 = new PipedReader(writer16);

    // part2
    PipedReader reader17 = new PipedReader(writer17);
    PipedReader reader18 = new PipedReader(writer18);
    PipedReader reader19 = new PipedReader(writer19);
    PipedReader reader20 = new PipedReader(writer20);

    PipedReader reader21 = new PipedReader(writer21);
    PipedReader reader22 = new PipedReader(writer22);
    PipedReader reader23 = new PipedReader(writer23);
    PipedReader reader24 = new PipedReader(writer24);

    PipedReader reader25 = new PipedReader(writer25);
    PipedReader reader26 = new PipedReader(writer26);
    PipedReader reader27 = new PipedReader(writer27);
    PipedReader reader28 = new PipedReader(writer28);

    PipedReader reader29 = new PipedReader(writer29);
    PipedReader reader30 = new PipedReader(writer30);
    PipedReader reader31 = new PipedReader(writer31);
    PipedReader reader32 = new PipedReader(writer32);


    ExecutorService executor = Executors.newFixedThreadPool(64);
    CopyManager cm11 = new CopyManager((BaseConnection) con1);
    CopyManager cm12 = new CopyManager((BaseConnection) con2);
    CopyManager cm13 = new CopyManager((BaseConnection) con3);
    CopyManager cm14 = new CopyManager((BaseConnection) con4);;

    CopyManager cm1 = new CopyManager((BaseConnection) c);
    CopyManager cm2 = new CopyManager((BaseConnection) c2);
    CopyManager cm3 = new CopyManager((BaseConnection) c3);
    CopyManager cm4 = new CopyManager((BaseConnection) c4);


    CopyManager cmt1 = new CopyManager((BaseConnection) ct1);
    CopyManager cmt2 = new CopyManager((BaseConnection) ct2);
    CopyManager cmt3 = new CopyManager((BaseConnection) ct3);
    CopyManager cmt4 = new CopyManager((BaseConnection) ct4);

    Runnable copyTo1 = createDBWriter("test1", cm1, reader1);
    Runnable copyTo5 = createDBWriter("test1", cm1, reader5);
    Runnable copyTo9 = createDBWriter("test1", cm1, reader9);
    Runnable copyTo13 = createDBWriter("test1", cm1, reader13);

    Runnable copyTo2 = createDBWriter("test1", cm2, reader2);
    Runnable copyTo6 = createDBWriter("test1", cm2, reader6);
    Runnable copyTo10 = createDBWriter("test1", cm2, reader10);
    Runnable copyTo14 = createDBWriter("test1", cm2, reader14);

    Runnable copyTo3 = createDBWriter("test1", cm3, reader3);
    Runnable copyTo7 = createDBWriter("test1", cm3, reader7);
    Runnable copyTo11 = createDBWriter("test1", cm3, reader11);
    Runnable copyTo15 = createDBWriter("test1", cm3, reader15);

    Runnable copyTo4 = createDBWriter("test1", cm4, reader4);
    Runnable copyTo8 = createDBWriter("test1", cm4, reader8);
    Runnable copyTo12 = createDBWriter("test1", cm4, reader12);
    Runnable copyTo16 = createDBWriter("test1", cm4, reader16);

    // part2

    Runnable copyTo17 = createDBWriter("test2", cmt1, reader17);
    Runnable copyTo21 = createDBWriter("test2", cmt1, reader21);
    Runnable copyTo25 = createDBWriter("test2", cmt1, reader25);
    Runnable copyTo29 = createDBWriter("test2", cmt1, reader29);

    Runnable copyTo18 = createDBWriter("test2", cmt2, reader18);
    Runnable copyTo22 = createDBWriter("test2", cmt2, reader22);
    Runnable copyTo26 = createDBWriter("test2", cmt2, reader26);
    Runnable copyTo30 = createDBWriter("test2", cmt2, reader30);

    Runnable copyTo19 = createDBWriter("test2", cmt3, reader19);
    Runnable copyTo23 = createDBWriter("test2", cmt3, reader23);
    Runnable copyTo27 = createDBWriter("test2", cmt3, reader27);
    Runnable copyTo31 = createDBWriter("test2", cmt3, reader31);

    Runnable copyTo20 = createDBWriter("test2", cmt4, reader20);
    Runnable copyTo24 = createDBWriter("test2", cmt4, reader24);
    Runnable copyTo28 = createDBWriter("test2", cmt4, reader28);
    Runnable copyTo32 = createDBWriter("test2", cmt4, reader32);

    Runnable copyFrom1 = createDBReader(q1p1, cm11, writer1);
    Runnable copyFrom2 = createDBReader(q1p2, cm11, writer2);
    Runnable copyFrom3 = createDBReader(q1p3, cm11, writer3);
    Runnable copyFrom4 = createDBReader(q1p4, cm11, writer4);

    Runnable copyFrom5 = createDBReader(q1p1, cm12, writer5);
    Runnable copyFrom6 = createDBReader(q1p2, cm12, writer6);
    Runnable copyFrom7 = createDBReader(q1p3, cm12, writer7);
    Runnable copyFrom8 = createDBReader(q1p4, cm12, writer8);


    Runnable copyFrom9 = createDBReader(q1p1, cm13, writer9);
    Runnable copyFrom10 = createDBReader(q1p2, cm13, writer10);
    Runnable copyFrom11 = createDBReader(q1p3, cm13, writer11);
    Runnable copyFrom12 = createDBReader(q1p4, cm13, writer12);

    Runnable copyFrom13 = createDBReader(q1p1, cm14, writer13);
    Runnable copyFrom14 = createDBReader(q1p2, cm14, writer14);
    Runnable copyFrom15 = createDBReader(q1p3, cm14, writer15);
    Runnable copyFrom16 = createDBReader(q1p4, cm14, writer16);

    // part2

    Runnable copyFrom17 = createDBReader(q2p1, cm11, writer17);
    Runnable copyFrom18 = createDBReader(q2p2, cm11, writer18);
    Runnable copyFrom19 = createDBReader(q2p3, cm11, writer19);
    Runnable copyFrom20 = createDBReader(q2p4, cm11, writer20);

    Runnable copyFrom21 = createDBReader(q2p1, cm12, writer21);
    Runnable copyFrom22 = createDBReader(q2p2, cm12, writer22);
    Runnable copyFrom23 = createDBReader(q2p3, cm12, writer23);
    Runnable copyFrom24 = createDBReader(q2p4, cm12, writer24);

    Runnable copyFrom25 = createDBReader(q2p1, cm13, writer25);
    Runnable copyFrom26 = createDBReader(q2p2, cm13, writer26);
    Runnable copyFrom27 = createDBReader(q2p3, cm13, writer27);
    Runnable copyFrom28 = createDBReader(q2p4, cm13, writer28);


    Runnable copyFrom29 = createDBReader(q2p1, cm14, writer29);
    Runnable copyFrom30 = createDBReader(q2p2, cm14, writer30);
    Runnable copyFrom31 = createDBReader(q2p3, cm14, writer31);
    Runnable copyFrom32 = createDBReader(q2p4, cm14, writer32);

    List<Future<?>> futures = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    futures.add(executor.submit(copyFrom1));
    futures.add(executor.submit(copyFrom2));
    futures.add(executor.submit(copyFrom3));
    futures.add(executor.submit(copyFrom4));
    futures.add(executor.submit(copyFrom5));
    futures.add(executor.submit(copyFrom6));
    futures.add(executor.submit(copyFrom7));
    futures.add(executor.submit(copyFrom8));
    futures.add(executor.submit(copyFrom9));
    futures.add(executor.submit(copyFrom10));
    futures.add(executor.submit(copyFrom11));
    futures.add(executor.submit(copyFrom12));
    futures.add(executor.submit(copyFrom13));
    futures.add(executor.submit(copyFrom14));
    futures.add(executor.submit(copyFrom15));
    futures.add(executor.submit(copyFrom16));



    futures.add(executor.submit(copyTo1));
    futures.add(executor.submit(copyTo2));
    futures.add(executor.submit(copyTo3));
    futures.add(executor.submit(copyTo4));
    futures.add(executor.submit(copyTo5));
    futures.add(executor.submit(copyTo6));
    futures.add(executor.submit(copyTo7));
    futures.add(executor.submit(copyTo8));
    futures.add(executor.submit(copyTo9));
    futures.add(executor.submit(copyTo10));
    futures.add(executor.submit(copyTo11));
    futures.add(executor.submit(copyTo12));
    futures.add(executor.submit(copyTo13));
    futures.add(executor.submit(copyTo14));
    futures.add(executor.submit(copyTo15));
    futures.add(executor.submit(copyTo16));

    // part2

    /*
     * futures.add(executor.submit(copyFrom17)); futures.add(executor.submit(copyFrom18));
     * futures.add(executor.submit(copyFrom19)); futures.add(executor.submit(copyFrom20));
     * futures.add(executor.submit(copyFrom21)); futures.add(executor.submit(copyFrom22));
     * futures.add(executor.submit(copyFrom23)); futures.add(executor.submit(copyFrom24));
     * futures.add(executor.submit(copyFrom25)); futures.add(executor.submit(copyFrom26));
     * futures.add(executor.submit(copyFrom27)); futures.add(executor.submit(copyFrom28));
     * futures.add(executor.submit(copyFrom29)); futures.add(executor.submit(copyFrom30));
     * futures.add(executor.submit(copyFrom31)); futures.add(executor.submit(copyFrom32));
     * 
     * 
     * 
     * futures.add(executor.submit(copyTo17)); futures.add(executor.submit(copyTo18));
     * futures.add(executor.submit(copyTo19)); futures.add(executor.submit(copyTo20));
     * futures.add(executor.submit(copyTo21)); futures.add(executor.submit(copyTo22));
     * futures.add(executor.submit(copyTo23)); futures.add(executor.submit(copyTo24));
     * futures.add(executor.submit(copyTo25)); futures.add(executor.submit(copyTo26));
     * futures.add(executor.submit(copyTo27)); futures.add(executor.submit(copyTo28));
     * futures.add(executor.submit(copyTo29)); futures.add(executor.submit(copyTo30));
     * futures.add(executor.submit(copyTo31)); futures.add(executor.submit(copyTo32));
     */
    for (Future<?> future : futures) {
      future.get();
      executor.shutdown();

    }
    long stopTime = System.currentTimeMillis();
    System.out.println("Elapsed time of third mnachine was " + (stopTime - startTime)
        + " miliseconds.");
  }

  public Runnable createDBReader(final String sql, final CopyManager cm, final Writer writer) {
    return new Runnable() {
      @Override
      public void run() {
        try {
          cm.copyOut(String.format("COPY ( %s ) to STDOUT WITH (FORMAT csv)", sql), writer);
          writer.close();
          System.out.println("finish copy to side" + sql);
        } catch (SQLException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

      }
    };

  }

  public Runnable createDBWriter(final String tableName, final CopyManager cm, final Reader reader) {
    return new Runnable() {
      @Override
      public void run() {
        // TODO Auto-generated method stub
        try {
          cm.copyIn(String.format("COPY %s from STDIN WITH (FORMAT csv)", tableName), reader);
          reader.close();
          System.out.println("finish copy from side");
        } catch (SQLException | IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    };
  }

}
