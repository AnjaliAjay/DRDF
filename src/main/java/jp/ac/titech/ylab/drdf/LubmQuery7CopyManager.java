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

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class LubmQuery7CopyManager {
  @Option(name="-m", required=true, usage="mydb connection")
  private String mydb;

  @Option(name="-i", required=true, usage="my server id number")
  private int myid;

  @Option(name="-d", required=true, usage="list of destinations")
  private String[] destinations;

  private static final String query1Template = "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 where T1.subject=T2.subject and T1.predicate='rdf:type' and T1.object='<ub:UndergraduateStudent>' and T2.predicate="
      + " 'ub:takesCourse' and abs(T2.tripleid %% %d) = %d";
  
  private static final String query2Template = "SELECT T2.tripleid triple1,T1.subject as course,T2.subject as student from rdfhashedbysubject T1,rdfhashedbyobject T2,rdfhashedbyobject T3 where T1.subject=T2.object and T2.object=T3.object"
      + " and T1.predicate='rdf:type' and T1.object='<ub:Course>' and T2.predicate='ub:takesCourse' and T3.subject='<http://www.Department0.University0.edu/AssociateProfessor0>' and T3.predicate='ub:teacherOf' and abs(T2.tripleid %% %d) = %d";

  private static final String query3Template = "SELECT Y.student, Y.course from X,Y where X.tripleid = Y.tripleid";

  public static void main(String args[]) throws Exception {
    LubmQuery7CopyManager app = new LubmQuery7CopyManager();
    
    CmdLineParser parser = new CmdLineParser(app);
    parser.parseArgument(args);
    app.run();
  }

  private LubmQuery7CopyManager() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    }
  }

  public LubmQuery7CopyManager(String mydb, int myid, String[] destinations) {
    this();
    this.mydb = mydb;
    this.myid = myid;
    this.destinations = destinations;
  }

  public void createTables(String[] destinations)
      throws Exception {
    for (String dest : destinations) {
      try (Connection conn = DriverManager.getConnection(dest + "test1", "postgres", "root")) {
        Statement stmt = conn.createStatement();
        stmt.execute("create table if not exists test1(tripleid integer);");
      }
      try (Connection conn = DriverManager.getConnection(dest + "test2", "postgres", "root")) {
        Statement stmt = conn.createStatement();
        stmt.execute("create table if not exists test2(tripleid integer,course varchar(100),student varchar(100));");
      }
    }
  }

  public void dropTables(String[] destinations) throws Exception {
    for (String dest : destinations) {
      try (Connection conn = DriverManager.getConnection(dest + "test1", "postgres", "root")) {
        Statement stmt = conn.createStatement();
        stmt.execute("drop table if exists test1;");
      }
      try (Connection conn = DriverManager.getConnection(dest + "test2", "postgres", "root")) {
        Statement stmt = conn.createStatement();
        stmt.execute("drop table if exists test2;");
      }
    }
  }
  
  public void run() throws Exception {
    if (myid == 0) {
      createTables(destinations);
    }
    
    List<Runnable> taskQueue = new ArrayList<Runnable>();
    buildExecutionPlan(mydb, myid, destinations, taskQueue);
    
    ExecutorService executor = null;
    try {
      executor = Executors.newFixedThreadPool(destinations.length * 2);
      execute(executor, taskQueue);
    } finally {    
      executor.shutdown();
    }
  }
  
  /**
   * 
   * @param mydb connection for the local database. ex. "jdbc:postgresql://localhost/server?_lumb50"
   * @param destinations servers ex. ["jdbc:postgresql://192.168.172.174:5432/", ...]
   * @param taskQueue
   * @throws Exception
   */
  private void buildExecutionPlan(String mydb, int myid, String[] destinations, List<Runnable> taskQueue) throws Exception {
    Connection srcCon = DriverManager.getConnection(mydb, "postgres", "root");
    int numberOfServers = destinations.length;
    // Query1 -> Test1
    for (int i = 0; i<destinations.length; i++) {
      String dest = destinations[i];
      String tableName = "test1";
      Connection destCon = DriverManager.getConnection(dest + tableName, "postgres", "root");
      int destID = (myid + i) % numberOfServers;
      submitCopyTask(srcCon, destCon, String.format(query1Template, numberOfServers, destID), tableName, taskQueue);
    }
    
    // Query2 -> Test2
    for (int i = 0; i<destinations.length; i++) {
      String dest = destinations[i];
      String tableName = "test2";
      Connection destCon = DriverManager.getConnection(dest + tableName, "postgres", "root");
      int destID = (myid + i) % numberOfServers;
      submitCopyTask(srcCon, destCon, String.format(query2Template, numberOfServers, destID), tableName, taskQueue);
    }
  }
  
  
  private void submitCopyTask(Connection srcCon, Connection destCon, String query, String tableName, List<Runnable> taskQueue) throws Exception {
    PipedWriter writer = new PipedWriter();
    PipedReader reader = new PipedReader(writer);

    CopyManager copySrcManager = new CopyManager((BaseConnection) srcCon);
    CopyManager copyDestManager = new CopyManager((BaseConnection) destCon);

    Runnable copyFromTask = createDBReader(query, copySrcManager, writer);
    Runnable copyToTask = createDBWriter(tableName, copyDestManager, reader);

    taskQueue.add(copyFromTask);
    taskQueue.add(copyToTask);
  }
  
  private void execute(ExecutorService executor, List<Runnable> taskQueue) throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    
    for (Runnable task : taskQueue) {
      futures.add(executor.submit(task));
    }
    
    for (Future<?> future : futures) {
      future.get();
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
