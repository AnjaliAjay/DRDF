package jp.ac.titech.ylab.drdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RDFDistributionServer {
    static String driverName = "org.postgresql.Driver";
    static String connectionName1 = "jdbc:postgresql://192.168.172.174:5432/server1?user=postgres&password=root&ssl=true";
    static String connectionName2 = "jdbc:postgresql://192.168.172.175:5432/server2?user=postgres&password=root&ssl=true";
    static String connectionName3 = "jdbc:postgresql://192.168.172.176:5432/server3?user=postgres&password=root&ssl=true";
    static String connectionName4 = "jdbc:postgresql://192.168.172.177:5432/server4?user=postgres&password=root&ssl=true";
    static String filename = "sp2b.n3";
    static String sqlSub = "create table rdfHashedbySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))";
    static String sqlObj = "create table rdfHashedbyObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))";
    private static int tripleVal = 0;
    public static HashMap<String, Long> hashvalues = new HashMap<String, Long>();
    public static HashMap<String, Long> hashvaluesSubject = new HashMap<String, Long>();
    static int numberOfMachines = 4;

    public static void distribute(String triple, Long hashOb, Long hashSub, String subject, String predicate, String object, Statement stat1, Statement stat2, Statement stat3, Statement stat4, Integer tripleVal) throws Exception {
        long hashValueOb = hashOb % numberOfMachines;
        long finalhashValueOb = (hashValueOb < 0 ? -hashValueOb : hashValueOb);
        long hashValueSub = hashSub % numberOfMachines;
        long finalhashValueSub = (hashValueSub < 0 ? -hashValueSub : hashValueSub);
        System.out.println("triple is" + triple);
        System.out.println("hash code of subject is" + finalhashValueOb);
        System.out.println("hash code of object is" + finalhashValueSub);
        System.out.println("-----------------------------------------------");
        String insertHashObject = "INSERT INTO rdfHashedbyObject VALUES ( "
                + "'" + tripleVal + "', "
                + "'" + subject + "', "
                + "'" + predicate + "', "
                + "'" + object + "')";

        String insertHashSubject = "INSERT INTO rdfHashedbySubject VALUES ( "
                + "'" + tripleVal + "', "
                + "'" + subject + "', "
                + "'" + predicate + "', "
                + "'" + object + "')";
        
        /*put all triples with hashofobject=1 to table1, similarly, with hashvalue 2 to 
        table2 and hash value 3 to table 3*/
        if (finalhashValueOb == 0)
            stat1.executeUpdate(insertHashObject);
        else if (finalhashValueOb == 1)
            stat2.executeUpdate(insertHashObject);
        else if (finalhashValueOb == 2)
            stat3.executeUpdate(insertHashObject);
        else
            stat4.execute(insertHashObject);
        /*put all triples with hashofsubject=1 to table1, similarly, with hashvalue 2 to 
        table2 and hash value 3 to table 3*/
        if (finalhashValueSub == 0)
            stat1.executeUpdate(insertHashSubject);
        else if (finalhashValueSub == 1)
            stat2.executeUpdate(insertHashSubject);
        else if (finalhashValueOb == 2)
            stat3.executeUpdate(insertHashObject);
        else
            stat4.executeUpdate(insertHashSubject);

    }

    public static void main(String[] args) throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(new File(
                filename)));
        String inputLine = null;
        String subject = null;
        String object = null;
        String predicate = null;
        try {
            Class.forName(driverName);
            Connection conn1 = DriverManager.
                    getConnection(connectionName1);
            System.out.println("Opened database successfully");
            Statement stat1 = conn1.createStatement();
            Connection conn2 = DriverManager.
                    getConnection(connectionName2);
            System.out.println("Opened database successfully");
            Statement stat2 = conn2.createStatement();
            Connection conn3 = DriverManager.
                    getConnection(connectionName3);
            System.out.println("Opened database successfully");
            Statement stat3 = conn3.createStatement();
            Connection conn4 = DriverManager.
                    getConnection(connectionName4);
            System.out.println("Opened database successfully");
            Statement stat4 = conn4.createStatement();

            stat1.executeUpdate(sqlSub);
            stat1.executeUpdate(sqlObj);

            stat2.executeUpdate(sqlSub);
            stat2.executeUpdate(sqlObj);

            stat3.executeUpdate(sqlSub);
            stat3.executeUpdate(sqlObj);

            stat4.executeUpdate(sqlSub);
            stat4.executeUpdate(sqlObj);

            while ((inputLine = reader.readLine()) != null) {
                String[] triples = inputLine.split("\\n");
                //iterate through string array
                for (String word : triples) {
                    int k = word.indexOf(" ");
                    String subject1 = word.substring(0, k);
                    String regex = ("^<|>$");
                    Matcher m = Pattern.compile(regex).matcher(subject1);
                    if (m.find())
                        subject = subject1.substring(1, subject1.length() - 1);
                    else
                        subject = subject1;
                    String predicateandobj = word.substring(k + 1);
                    int k1 = predicateandobj.indexOf(" ");
                    predicate = predicateandobj.substring(0, k1);
                    String ob = predicateandobj.substring(k1 + 1);
                    String object1 = ob.substring(0, ob.length() - 1);
                    Matcher m1 = Pattern.compile(regex).matcher(object1);
                    if (m1.find())
                        object = object1.substring(1, object1.length() - 1);
                    else
                        object = object1;
                    MessageDigest mdObject = MessageDigest.getInstance("MD5");
                    mdObject.update(object.getBytes());
                    byte[] objectDigest = mdObject.digest();
                    long hObject = 0;
                    for (int i = 0; i < 4; i++) {
                        hObject <<= 8;
                        hObject |= ((int) objectDigest[i]) & 0xFF;
                    }
                    MessageDigest mdSubject = MessageDigest.getInstance("MD5");
                    mdSubject.update(subject.getBytes());
                    byte[] subjectDigest = mdSubject.digest();
                    long hSubject = 0;
                    for (int i = 0; i < 4; i++) {
                        hSubject <<= 8;
                        hSubject |= ((int) subjectDigest[i]) & 0xFF;
                    }
                    tripleVal = tripleVal + 1;
                    distribute(word, hObject, hSubject, subject, predicate, object, stat1, stat2, stat3, stat4, tripleVal);


                }


            }
            reader.close();
            stat1.close();
            stat2.close();
            stat3.close();
            stat4.close();
            conn1.close();
            conn2.close();
            conn3.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }
}
