

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
public class Copytry {
  
  public static void main( String args[] ) throws Exception
  {
    Copytry app = new Copytry();
    Connection c = DriverManager
        .getConnection("jdbc:postgresql://192.168.172.176:5432/server3_lubm1", "postgres",
              "root");
    Connection c2 = DriverManager
        .getConnection("jdbc:postgresql://192.168.172.175:5432/server2_lubm1", "postgres",
              "root");
 //   app.createTable(c,c2);
    
    app.copy(c,c2);
  }

  public void createTable(Connection c,Connection c2) throws Exception {
    Statement stmt = c.createStatement();
    stmt.execute("create table t4(id integer);");
     Statement stmt2 = c2.createStatement();
    stmt2.execute("create table t4(id integer);");
  }

  public void copy(Connection c, Connection c2) throws Exception {
    PipedWriter writer = new PipedWriter();
    PipedWriter writer1 = new PipedWriter();
    PipedReader reader = new PipedReader(writer);
    PipedReader reader1 = new PipedReader(writer1);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    
    CopyManager cm = new CopyManager((BaseConnection) c);
    CopyManager cm1 = new CopyManager((BaseConnection) c2);
    
    Runnable copyTo = createDBWriter("t4", cm, reader);
    Runnable copyTo1 = createDBWriter("t4", cm1, reader1);
    Runnable copyFrom = createDBReader("select T2.tripleId triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2"
    +" where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate='ub:takesCourse' and abs(T2.tripleid%2)=0",cm, writer);
    Runnable copyFrom1 = createDBReader("select T2.tripleId triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2"
    +" where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate='ub:takesCourse' and abs(T2.tripleid%2)=1", cm, writer1);
    List<Future<?>> futures = new ArrayList<>();
    futures.add(executor.submit(copyFrom));
    futures.add(executor.submit(copyFrom1));
    futures.add(executor.submit(copyTo));
    futures.add(executor.submit(copyTo1));
    for (Future<?> future : futures) {
      future.get();
    }
    executor.shutdown();
    
  }
  public Runnable createDBReader(final String sql, final CopyManager cm, final Writer writer) {
    System.out.println("entered");
    return new Runnable() {

      @Override
      public void run() {
        try {
          cm.copyOut(String.format("COPY ( %s ) to STDOUT", sql), writer);
          writer.close();
          System.out.println("finish copy to side");
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
      System.out.println("entered2");
      return new Runnable() {

        @Override
        public void run() {
          // TODO Auto-generated method stub
          try {
            cm.copyIn(String.format("COPY %s from STDIN", tableName), reader);
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

