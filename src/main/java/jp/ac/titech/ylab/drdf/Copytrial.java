package jp.ac.titech.ylab.drdf;
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
public class Copytrial {
  
  public static void main( String args[] ) throws Exception
  {
    Copytrial app = new Copytrial();
    Connection c = DriverManager
        .getConnection("jdbc:postgresql://localhost:5432/test",
        "postgres", "root");
    //app.createTable(c);
    //app.copy(c);
    copyExample(c);
  }

  private static void copyExample(Connection c2) throws SQLException, ClassNotFoundException, IOException {
    //Connection c = null;
   
    
    StringBuilder sb=new StringBuilder();
    CopyManager cm = new CopyManager((BaseConnection) c2);
    PushbackReader reader = new PushbackReader( new StringReader(""), 10000 );
    
      Class.forName("org.postgresql.Driver");
     
      Statement stmt = c2.createStatement();
     // stmt.execute("create table empv2(id integer,name varchar(50));");
      ResultSet rs = stmt.executeQuery( "SELECT * FROM employee;" );
      while ( rs.next() ) {
        String  name = rs.getString("empname");
        int id=rs.getInt("empid");
        sb.append(id).append(",'").append(name).append("'\n");
        System.out.println(sb);
        reader.unread(sb.toString().toCharArray());
        cm.copyIn("COPY empv1 FROM STDIN WITH csv",reader);
        sb.delete(0,sb.length());
      }
  }
  
  public void createTable(Connection c) throws Exception {
    Statement stmt = c.createStatement();
    stmt.execute("create table empv2(id integer,name varchar(100));");
  }

  public void copy(Connection c) throws Exception {
    PipedWriter writer = new PipedWriter();
    PipedReader reader = new PipedReader(writer);
    
    ExecutorService executor = Executors.newFixedThreadPool(2);
    
    CopyManager cm = new CopyManager((BaseConnection) c);
    
    Runnable copyTo = createDBWriter("empv2", cm, reader);
    Runnable copyFrom = createDBReader("SELECT * FROM employee", cm, writer);
    
    List<Future<?>> futures = new ArrayList<>();
    futures.add(executor.submit(copyFrom));
    futures.add(executor.submit(copyTo));
    
    for (Future<?> future : futures) {
      future.get();
    }
    executor.shutdown();
    
  }
  public Runnable createDBReader(String sql, final CopyManager cm, final Writer writer) {
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
  
    public Runnable createDBWriter(String tableName, final CopyManager cm, final Reader reader) {
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
