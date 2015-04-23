package jp.ac.titech.ylab.drdf;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
class Slave5 implements Callable<Set<String>> {
 String q,q1=null;
 Connection c11=null;
 
  public Slave5(String query1, String query2, Connection c1) {
    // TODO Auto-generated constructor stub
    q=query1;
    q1=query2;
    c11=c1;
  }

  public Set<String> call() throws SQLException {


    PreparedStatement stat1 = null;
    PreparedStatement stat12 = null;
    ResultSet rs1=null;
    ResultSet rs1part2 = null;
    Set<String>union=new HashSet<String>();

   
    System.out.println("connection created");
    stat1 = c11.prepareStatement(q);
    rs1 = stat1.executeQuery();
    
    stat12 = c11.prepareStatement(q1);
    rs1part2 = stat12.executeQuery();
   while(rs1.next()){
     union.add(rs1.getString("pred"));
   }

   while(rs1part2.next()){
     if(!union.contains(rs1part2.getString("pred"))){
       union.add(rs1part2.getString("pred"));
     }
   }

  return(union);
 
  }
}
class Slave6 implements Callable<Set<String>> {
  String q,q1=null;
  Connection c22=null;
  public Slave6(String query1, String query2, Connection c2) {
    // TODO Auto-generated constructor stub
    q=query1;
    q1=query2;
    c22=c2;
  }

  public Set<String> call() throws SQLException{
 
   

    PreparedStatement stat2 = null;
    PreparedStatement stat22 = null;
    ResultSet rs2=null;
    ResultSet rs2part2 = null;
    Set<String>union2=new HashSet<String>();

   
    System.out.println("connection created");
    stat2 = c22.prepareStatement(q);
    rs2 = stat2.executeQuery();
    
    stat22 = c22.prepareStatement(q1);
    rs2part2 = stat22.executeQuery();
   while(rs2.next()){
     union2.add(rs2.getString("pred"));
   }
 
   while(rs2part2.next()){
     if(!union2.contains(rs2part2.getString("pred"))){
       union2.add(rs2part2.getString("pred"));
     }
   }
 return(union2);

   
}
}
class Slave7 implements Callable<Set<String>> {
  String q,q1=null;
  Connection c33=null; 
  public Slave7(String query1, String query2, Connection c3) {
    // TODO Auto-generated constructor stub
    q=query1;
    q1=query2;
    c33=c3;
  }

  public Set<String> call() throws SQLException {


    PreparedStatement stat3 = null;
    PreparedStatement stat33 = null;
    ResultSet rs3=null;
    ResultSet rs3part2=null;
    Set<String>union3=new HashSet<String>();

    stat3 = c33.prepareStatement(q);
    rs3 = stat3.executeQuery();
    
    stat33 = c33.prepareStatement(q1);
    rs3part2 = stat33.executeQuery();
   
   while(rs3.next()){
     union3.add(rs3.getString("pred"));
   }
   while(rs3part2.next()){
     if(!union3.contains(rs3part2.getString("pred"))){
       union3.add(rs3part2.getString("pred"));
     }
   }
    return(union3);
 
  }
}
class Slave8 implements Callable<Set<String>> {
  String q,q1=null;
  Connection c44=null; 
  public Slave8(String query1, String query2, Connection c4) {
    // TODO Auto-generated constructor stub
    q=query1;
    q1=query2;
    c44=c4;
  }

  public Set<String> call() throws SQLException {

 
    PreparedStatement stat4 = null;
    PreparedStatement stat44 = null;
    ResultSet rs4=null;
    ResultSet rs4part2 = null;
    Set<String>union4=new HashSet<String>();
    stat4 = c44.prepareStatement(q);
    rs4 = stat4.executeQuery();
    
    stat44 = c44.prepareStatement(q1);
    rs4part2 = stat44.executeQuery();

   while(rs4.next()){
     union4.add(rs4.getString("pred"));
   }


   while(rs4part2.next()){
     if(!union4.contains(rs4part2.getString("pred"))){
       union4.add(rs4part2.getString("pred"));
     }
   }
    return(union4);
 
  }
}
public class UnionHashSet {

  public static void main(String[] args) throws ClassNotFoundException, InterruptedException, SQLException{
  
        Class.forName("org.postgresql.Driver");
       Set<String> set = new HashSet<String>();

       final String query1="SELECT DISTINCT b.predicate pred from RDFHASHEDBYSUBJECT a, RDFHASHEDBYOBJECT b WHERE a.subject = b.object"
        +" and a.predicate='rdf:type' and a.object='foaf:Person';";
       final String query2="SELECT DISTINCT b.predicate pred"
        +" from RDFHASHEDBYSUBJECT a, RDFHASHEDBYSUBJECT b"
        + " WHERE a.subject = b.subject and a.predicate='rdf:type' and a.object='foaf:Person';";
       
       
    final ExecutorService service;
  final Future<Set<String>>  task;
  final Future<Set<String>>  task1;
  final Future<Set<String>>  task2;
  final Future<Set<String>>  task3;

  service = Executors.newFixedThreadPool(4);        
  Connection c1 = null;
  task    = service.submit(new Slave5(query1,query2,c1));
  Connection c2 = null;
  task1 = service.submit(new Slave6(query1,query2,c2));
  Connection c3 = null;
  task2 = service.submit(new Slave7(query1,query2,c3));
  Connection c4 = null;
  task3 = service.submit(new Slave8(query1,query2,c4));
  try {
      Set<String> r1= new HashSet<String>();
      Set<String> r2= new HashSet<String>();
      Set<String> r3= new HashSet<String>();
      Set<String> r4= new HashSet<String>();
      // waits the 10 seconds for the Callable.call to finish.
      r1 = task.get();
      r2=task.get();
      r3=task.get();
      r4=task.get();
      set.addAll(r1);
      set.addAll(r2);
      set.addAll(r3);
      set.addAll(r4);
      Iterator<String> litr = set.iterator();
      while(litr.hasNext()) {
         Object element = litr.next();
         System.out.println(element);
      } 
     // System.out.println(str1);
  } catch(final ExecutionException ex) {
      ex.printStackTrace();
  } 

  service.shutdownNow();
}

}

