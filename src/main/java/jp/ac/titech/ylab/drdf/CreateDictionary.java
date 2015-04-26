package jp.ac.titech.ylab.drdf;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateDictionary {
    @Option(name = "-h")
    private boolean showHelp = false;

    @Option(name = "-d", usage = "driver name", required = false)
    private String driverName = "org.h2.Driver";

    @Option(name = "-c", usage = "connection name", required = false)
    private String connectionName = "jdbc:h2:~/dictionary";

    @Option(name = "-f", usage = "file name", required = false)
    private String filename = "D:/sp2b/bin/sp2bnoprefix.n3";

    public static void main(String[] args) throws Exception {
        CreateDictionary app = new CreateDictionary();
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

    public void run() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File(
                filename)));
        String inputLine = null;
        String words = null;
        int value = 0;
        Class.forName(driverName);
        Connection conn = DriverManager.
                getConnection(connectionName);
        Statement stat = conn.createStatement();
        // stat.execute("create table dictionary(triple varchar(5000), id int(255))");
        HashMap<String, Integer> dictionary = new HashMap<String, Integer>();
        while ((inputLine = reader.readLine()) != null) {

            String regex = ("([^\"]\\S*|\".+\\n?\\.)\\s*");
            Matcher m = Pattern.compile(regex).matcher(inputLine);
            while (m.find()) {

                for (int i = 0; i < m.group().length(); i++) {
                    words = m.group();
                }

                if (!dictionary.containsKey(words))

                {
                    String insertStr = "INSERT INTO dictionary VALUES ( "
                            + "'" + words + "', "
                            + "'" + value + "')";
                    stat.execute(insertStr);
                    dictionary.put(words.substring(0, words.length() - 1), value);
                    value += 1;
                }


            }
        }

        reader.close();
    }

}

