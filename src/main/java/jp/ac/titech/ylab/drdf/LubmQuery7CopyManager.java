package jp.ac.titech.ylab.drdf;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LubmQuery7CopyManager {
    @Option(name = "-dataset", required = true, usage = "dataset")
    private String dataset;

    @Option(name = "-i", required = true, usage = "my server id number")
    private int myid;

    @Option(name = "-servers", required = true, usage = "list of servers")
    private String[] destinations = new String[]{
            "jdbc:postgresql://192.168.172.174:5432/",
            "jdbc:postgresql://192.168.172.175:5432/",
            "jdbc:postgresql://192.168.172.176:5432/",
            "jdbc:postgresql://192.168.172.177:5432/"
    };


    private static final String query1Template =
            "SELECT T2.tripleid triple1 from rdfhashedbysubject T1,rdfhashedbysubject T2 " +
                    "where T1.subject=T2.subject " +
                    "and T1.predicate='rdf:type' " +
                    "and T1.object='<ub:UndergraduateStudent>' " +
                    "and T2.predicate='ub:takesCourse' " +
                    "and abs(T2.tripleid %% %d) = %d";

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

    public LubmQuery7CopyManager(String dataset, int myid) {
        this();
        this.dataset = dataset;
        this.myid = myid;
    }

    /**
     * connection for the local database. ex. "jdbc:postgresql://localhost/server?_lumb50"
     *
     * @param id
     * @param dataset
     * @return
     */
    private String createDBName(int id, String dataset) {
        return destinations[id] + String.format("server%d_%s", id + 1, dataset);
    }

    public void createTables()
            throws Exception {
        for (int i = 0; i < destinations.length; i++) {
            try (Connection conn = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root")) {
                Statement stmt = conn.createStatement();
                stmt.execute("create table if not exists test1(tripleid integer);");
            }
            try (Connection conn = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root")) {
                Statement stmt = conn.createStatement();
                stmt.execute("create table if not exists test2(tripleid integer,course varchar(100),student varchar(100));");
            }
        }
    }

    public void dropTables() throws Exception {
        for (int i = 0; i < destinations.length; i++) {
            try (Connection conn = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root")) {
                Statement stmt = conn.createStatement();
                stmt.execute("drop table if exists test1;");
            }
            try (Connection conn = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root")) {
                Statement stmt = conn.createStatement();
                stmt.execute("drop table if exists test2;");
            }
        }
    }

    public String run() throws Exception {
        StringBuilder logBuf = new StringBuilder();
        if (myid == 0) {
            createTables();
        }

        List<Runnable> taskQueue = new ArrayList<Runnable>();
        buildExecutionPlan(dataset, myid, destinations, taskQueue, logBuf);

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(destinations.length * 2);
            execute(executor, taskQueue);
            return logBuf.toString();
        } finally {
            executor.shutdown();
        }
    }

    /**
     * @param dataset      dataset name
     * @param destinations servers ex. ["jdbc:postgresql://192.168.172.174:5432/", ...]
     * @param taskQueue
     * @throws Exception
     */
    private void buildExecutionPlan(String dataset, int myid, String[] destinations, List<Runnable> taskQueue, StringBuilder logBuf) throws Exception {
        int numberOfServers = destinations.length;
        // Query1 -> Test1
        for (int i = 0; i < destinations.length; i++) {
            String tableName = "test1";
            Connection srcCon = DriverManager.getConnection(createDBName(myid, dataset), "postgres", "root");
            Connection destCon = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root");
            int destID = (myid + i) % numberOfServers;
            submitCopyTask(srcCon, destCon, String.format(query1Template, numberOfServers, destID), tableName, taskQueue, logBuf);
        }

        // Query2 -> Test2
        for (int i = 0; i < destinations.length; i++) {
            String tableName = "test2";
            Connection srcCon = DriverManager.getConnection(createDBName(myid, dataset), "postgres", "root");
            Connection destCon = DriverManager.getConnection(createDBName(i, dataset), "postgres", "root");
            int destID = (myid + i) % numberOfServers;
            submitCopyTask(srcCon, destCon, String.format(query2Template, numberOfServers, destID), tableName, taskQueue, logBuf);
        }
    }


    private void submitCopyTask(Connection srcCon, Connection destCon, String query, String tableName, List<Runnable> taskQueue, StringBuilder logBuf) throws Exception {
        PipedWriter writer = new PipedWriter();
        PipedReader reader = new PipedReader(writer, 4096);

        CopyManager copySrcManager = new CopyManager((BaseConnection) srcCon);
        CopyManager copyDestManager = new CopyManager((BaseConnection) destCon);

        Runnable copyFromTask = createDBReader(query, copySrcManager, writer, logBuf);
        Runnable copyToTask = createDBWriter(tableName, copyDestManager, reader, logBuf);

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
        System.out.println("Elapsed time of third machine was " + (stopTime - startTime)
                + " miliseconds.");
    }

    public Runnable createDBReader(final String sql, final CopyManager cm, final Writer writer, final StringBuilder logBuf) {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    long startTime = System.nanoTime();
                    cm.copyOut(String.format("COPY ( %s ) to STDOUT WITH (FORMAT text)", sql), writer);
                    writer.close();
                    long endTime = System.nanoTime();
                    logBuf.append(String.format("copyTo task takes %d ns.\n", endTime - startTime));
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

    public Runnable createDBWriter(final String tableName, final CopyManager cm, final Reader reader, final StringBuilder logBuf) {
        return new Runnable() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                try {
                    long startTime = System.nanoTime();
                    cm.copyIn(String.format("COPY %s from STDIN WITH (FORMAT text)", tableName), reader);
                    reader.close();
                    long endTime = System.nanoTime();
                    logBuf.append(String.format("Copy from %s task takes %d ns.\n", tableName, endTime - startTime));
                    System.out.println("finish copy from side");
                } catch (SQLException | IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        };
    }

}
