package jp.ac.titech.ylab.drdf;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closeables;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DataLoadLubm {

    @Option(name="-help")
    private boolean showHelp = false;

    @Option(name="-input", usage="filename to input")
    private String filename = "combined_univ0.txt";

    @Option(name="-dataset", usage="dataset name")
    private String dataset = "lubm1";

    @Option(name="-slaves", usage="list of slaves")
    private String[] slaves = {
            "jdbc:postgresql://192.168.172.174:5432",
            "jdbc:postgresql://192.168.172.175:5432",
            "jdbc:postgresql://192.168.172.176:5432",
            "jdbc:postgresql://192.168.172.177:5432"
    };

    @Option(name="-user", usage="database user name")
    private String dbUser = "postgres";

    @Option(name="-password", usage="database password")
    private String dbPassword = "root";

    @Option(name="-batchsize", usage="batch execution size")
    private int batchSize = 1000;

    public static void main(String[] args) throws Exception {
        DataLoadLubm app = new DataLoadLubm();
        CmdLineParser parser = new CmdLineParser(app);
        parser.parseArgument(args);

        if (app.showHelp) {
            parser.printUsage(System.out);
            System.exit(0);
        }

        long startTime = System.currentTimeMillis();
        app.run();
        long stopTime = System.currentTimeMillis();
        System.out.println(String.format("Elapsed time was %d milliseconds.", stopTime - startTime));

        System.exit(0);
    }

    public void run() throws Exception {
        Connection[] connections = createConnections();
        try {
            createTables(connections);
            batchInsert(readFile(), connections);
            waitForAllTaskCompletion();
        } finally {
            closeConnections(connections);
            executor.shutdown();
        }
    }

    private Connection[] createConnections() throws Exception {
        Connection[] connections = new Connection[slaves.length];
        Class.forName("org.postgresql.Driver");
        for (int i = 0; i < connections.length; i++) {
            connections[i] = DriverManager.getConnection(generateConnectionName(slaves[i], i), dbUser, dbPassword);
        }
        return connections;
    }

    private void closeConnections(Connection[] connections) {
        for (Connection connection : connections) {
            try {
                connection.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
    }

    private String generateConnectionName(String slave, int id) {
        return String.format("%s/server%d_%s", slave, id, dataset);
    }

    private void createTables(Connection[] connections) throws Exception {
        for (Connection connection: connections) {
            try(Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE IF NOT EXISTS rdfHashedBySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
                statement.execute("CREATE TABLE IF NOT EXISTS rdfHashedByObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
            }
        }
    }

    private Iterable<Triple> readFile() throws IOException {
        final BufferedReader reader = new BufferedReader(new FileReader(new File(
                filename)));
        return new Iterable<Triple>() {

            @Override
            public Iterator<Triple> iterator() {
                return new AbstractIterator<Triple>() {
                    private int n = 0;

                    @Override
                    protected Triple computeNext() {
                        try {
                            String inputLine = reader.readLine();
                            if (inputLine == null) {
                                Closeables.closeQuietly(reader);
                                return endOfData();
                            }
                            return parse(n++, inputLine);
                        } catch (IOException e) {
                            Closeables.closeQuietly(reader);
                            return endOfData();
                        }
                    }
                };
            }
        };
    }

    private final static HashMap<String, String> PREFIXES;
    static {
        PREFIXES = new HashMap<String, String>();
        PREFIXES.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:");
        PREFIXES.put("http://www.w3.org/2002/07/owl#", "owl:");
        PREFIXES.put("http://swat.cse.lehigh.edu/onto/univ-bench.owl#", "ub:");
        PREFIXES.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs:");
    }


    private Triple parse(int id, String word) {
        int k = word.indexOf(" ");
        String subject = word.substring(0, k);


        String predicateAndObject = word.substring(k + 1);
        int k1 = predicateAndObject.indexOf(" ");
        String predicate = predicateAndObject.substring(1, k1 - 1);
        String ob = predicateAndObject.substring(k1 + 1);
        String object = ob.substring(0, ob.length() - 2);

        for (Map.Entry<String, String> hEntry : PREFIXES.entrySet()) {
            String prefixKey = hEntry.getKey().toString();
            String prefixValue = hEntry.getValue().toString();
            if (subject.contains(prefixKey)) {
                subject = subject.replaceAll(prefixKey, prefixValue);

            }
            if (predicate.contains(prefixKey)) {
                predicate = predicate.replaceAll(prefixKey, prefixValue);

            }
            if (object.contains(prefixKey)) {
                object = object.replaceAll(prefixKey, prefixValue);

            }
        }
        return new Triple(id, subject, predicate, object);
    }

    private int calculateDestination(String entry) {
        long hash = calculateHash(entry) % slaves.length;
        int dest = (int) (hash < 0 ? -hash : hash);
        return dest;
    }

    private static final MessageDigest MD5;
    static {
        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError();
        }
    }

    private long calculateHash(String entry) {
        MD5.reset();
        MD5.update(entry.getBytes());
        byte[] digest = MD5.digest();
        long hash = 0;
        for (int i = 0; i < 4; i++) {
            hash <<= 8;
            hash |= ((int) digest[i]) & 0xFF;
        }
        return hash;
    }


    private void batchInsert(Iterable<Triple> triples, Connection[] connections) throws Exception {
        final PreparedStatement[] insertToRdfHashedBySubject = new PreparedStatement[connections.length];
        final PreparedStatement[] insertToRdfHashedByObject = new PreparedStatement[connections.length];

        for (int i = 0; i < connections.length; i++) {
            insertToRdfHashedBySubject[i] = connections[i].prepareStatement("INSERT INTO rdfHashedBySubject VALUES (?, ?, ?, ?)");
            insertToRdfHashedByObject[i] = connections[i].prepareStatement("INSERT INTO rdfHashedByObject VALUES (?, ?, ?, ?)");
        }

        int[] sCount = new int[connections.length];
        int[] oCount = new int[connections.length];

        for (Triple triple : triples) {
            int slaveForSubject = calculateDestination(triple.subject);

            insertToRdfHashedBySubject[slaveForSubject].setInt(1, triple.id);
            insertToRdfHashedBySubject[slaveForSubject].setString(2, triple.subject);
            insertToRdfHashedBySubject[slaveForSubject].setString(3, triple.predicate);
            insertToRdfHashedBySubject[slaveForSubject].setString(4, triple.object);
            insertToRdfHashedBySubject[slaveForSubject].addBatch();
            sCount[slaveForSubject]++;
            if (sCount[slaveForSubject] % batchSize == 0) {
                submitTask(insertToRdfHashedBySubject[slaveForSubject]);
                insertToRdfHashedBySubject[slaveForSubject] = connections[slaveForSubject].prepareStatement("INSERT INTO rdfHashedBySubject VALUES (?, ?, ?, ?)");
            }

            int slaveForObject = calculateDestination(triple.object);
            insertToRdfHashedByObject[slaveForObject].setInt(1, triple.id);
            insertToRdfHashedByObject[slaveForObject].setString(2, triple.subject);
            insertToRdfHashedByObject[slaveForObject].setString(3, triple.predicate);
            insertToRdfHashedByObject[slaveForObject].setString(4, triple.object);
            oCount[slaveForObject]++;
            if (oCount[slaveForObject] % batchSize == 0) {
                submitTask(insertToRdfHashedByObject[slaveForObject]);
                insertToRdfHashedByObject[slaveForObject].executeBatch();
            }
        }
        for (PreparedStatement statement : insertToRdfHashedBySubject) {
            submitTask(statement);
        }
        for (PreparedStatement statement : insertToRdfHashedByObject) {
            submitTask(statement);
        }
    }

    private ExecutorService executor = Executors.newFixedThreadPool(slaves.length * 2);
    private List<Future<?>> futures = new ArrayList<>();

    private void submitTask(final PreparedStatement statement) {
        futures.add(executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    statement.executeBatch();
                } catch (SQLException e) {
                    // do nothing
                    e.printStackTrace();
                }
            }
        }));
    }

    private void waitForAllTaskCompletion() {
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                // do nothing
                e.printStackTrace();
            } catch (ExecutionException e) {
                // do nothing
                e.printStackTrace();
            }
        }
    }
}

