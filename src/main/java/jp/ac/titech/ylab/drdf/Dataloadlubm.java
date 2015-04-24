package jp.ac.titech.ylab.drdf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.MessageDigest;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Dataloadlubm
{

  public static int numberOfTriples=0;
  public static String filename = "combined_univ0.txt";
    private static int tripleVal = 0;
    public static HashMap<String, Long> hashvalues = new HashMap<String, Long>();
    public static HashMap<String, Long> hashvaluesSubject = new HashMap<String, Long>();
    static int numberOfMachines = 4;
    StringBuilder sb = new StringBuilder();
    
    public static void distribute(String triple,Long hashOb,Long hashSub,String subject,String predicate,String object,Statement stat1,Statement stat2,Statement stat3,Statement stat4,Integer tripleVal) throws Exception
    {
        numberOfTriples+=1;
       
        long hashValueOb = hashOb % numberOfMachines;
        long finalhashValueOb = (hashValueOb<0 ? -hashValueOb : hashValueOb);
        long hashValueSub = hashSub % numberOfMachines;
        long finalhashValueSub = (hashValueSub<0 ? -hashValueSub : hashValueSub);
        System.out.println("number of triples is "+numberOfTriples);
       System.out.println("triple is"+triple);
      // System.out.println("subject "+subject);
       //System.out.println("predicate "+predicate);
      // System.out.println("object is "+object);
        System.out.println("hash code of subject is"+finalhashValueOb);
       System.out.println("hash code of object is"+finalhashValueSub);
        System.out.println("-----------------------------------------------");
        
        String insertHashObject = "INSERT INTO rdfHashedbyObject VALUES ( "
                +"'" +tripleVal +"', "
             +"'" +subject +"', " 
              +"'" +predicate +"', " 
             +"'" +object +"')";

        String insertHashSubject = "INSERT INTO rdfHashedbySubject VALUES ( "
                +"'" +tripleVal +"', "
                 +"'" +subject +"', " 
                  +"'" +predicate +"', " 
                  +"'" +object +"')";
       
        System.out.println(insertHashObject);
        System.out.println(insertHashSubject);
       // put all triples with hashofobject=1 to table1, similarly, with hashvalue 2 to 
       // table2 and hash value 3 to table 3
        if (finalhashValueOb == 0)
             stat1.execute(insertHashObject);
        else if(finalhashValueOb == 1)
             stat2.execute(insertHashObject);
        else if(finalhashValueOb == 2)
             stat3.execute(insertHashObject);
        else if(finalhashValueOb == 3)
             stat4.execute(insertHashObject);
        //put all triples with hashofsubject=1 to table1, similarly, with hashvalue 2 to 
        //table2 and hash value 3 to table 3
        if (finalhashValueSub == 0)
             stat1.execute(insertHashSubject);
        else if(finalhashValueSub == 1)
             stat2.execute(insertHashSubject);
        else if(finalhashValueSub == 2)
             stat3.execute(insertHashSubject);
        else if(finalhashValueSub == 3)
             stat4.execute(insertHashSubject);

    }
    

    public static void main(String[] args) throws Exception {

        //BufferedReader reader = new BufferedReader(new FileReader(new File("D:/sp2b/bin/sp2b.n3")));
        BufferedReader reader = new BufferedReader(new FileReader(new File(
                filename)));
        String inputLine = null;
        String subject = null;
        String object = null;
        String predicate = null;
       
        HashMap<String, String> hmap = new HashMap<String,String>();

        hmap.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf:");
        hmap.put("http://www.w3.org/2002/07/owl#","owl:");
        hmap.put("http://swat.cse.lehigh.edu/onto/univ-bench.owl#","ub:");
        hmap.put("http://www.w3.org/2000/01/rdf-schema#","rdfs:");
        Class.forName("org.postgresql.Driver");

                    Connection conn1 = DriverManager
                        .getConnection(
                              "jdbc:postgresql://192.168.172.174:5432/server1_lubm1", "postgres",
                             "root");
               Statement stat1=conn1.createStatement();
                    Connection conn2 = DriverManager
                        .getConnection(
                              "jdbc:postgresql://192.168.172.175:5432/server2_lubm1", "postgres",
                             "root");
                    Statement stat2=conn2.createStatement();
                    Connection conn3 = DriverManager
                        .getConnection(
                              "jdbc:postgresql://192.168.172.176:5432/server3_lubm1", "postgres",
                             "root");
                    Statement stat3=conn3.createStatement();
                    Connection conn4 = DriverManager
                        .getConnection(
                              "jdbc:postgresql://192.168.172.177:5432/server4_lubm1", "postgres",
                             "root");  
                    Statement stat4=conn4.createStatement();
/*       stat1.execute("create table rdfHashedbySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat1.execute("create table rdfHashedbyObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       
       stat2.execute("create table rdfHashedbySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat2.execute("create table rdfHashedbyObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       
       stat3.execute("create table rdfHashedbySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat3.execute("create table rdfHashedbyObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       
       stat4.execute("create table rdfHashedbySubject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat4.execute("create table rdfHashedbyObject(tripleId int, subject varchar(255), predicate varchar(255),object varchar(5000))");
  */
    long startTime1 = System.currentTimeMillis();     
        while((inputLine = reader.readLine()) != null)
        {
            String[] triples = inputLine.split("\\n");
            //iterate through string array
            for(String word : triples)
            {
                int k = word.indexOf(" ");
                subject =word.substring(0,k);
               
             
                String predicateandobj = word.substring(k+1);
                int k1 = predicateandobj.indexOf(" ");
                predicate = predicateandobj.substring(1,k1-1);
                String ob = predicateandobj.substring(k1+1);
                object = ob.substring(0,ob.length()-2);
                tripleVal=tripleVal+1;
                for(Map.Entry<String, String> hEntry : hmap.entrySet()){
                  String prefixKey=hEntry.getKey().toString();
                  String prefixValue=hEntry.getValue().toString();
                      if(subject.contains(prefixKey))
                      {    
                          subject = subject.replaceAll(prefixKey, prefixValue);
                         
                      }
                      if(predicate.contains(prefixKey))
                      {
                          predicate = predicate.replaceAll(prefixKey, prefixValue);
                         
                      }
                      if(object.contains(prefixKey))
                      {
                          object = object.replaceAll(prefixKey, prefixValue);
                         
                      }
                  }
                MessageDigest mdObject = MessageDigest.getInstance("MD5");
                mdObject.update(object.getBytes());
                byte[] objectDigest = mdObject.digest();    
                long hObject = 0;
                for (int i = 0; i < 4; i++) 
                {
                        hObject <<= 8;
                        hObject |= ((int) objectDigest[i]) & 0xFF;
                }
                MessageDigest mdSubject = MessageDigest.getInstance("MD5");
                mdSubject.update(subject.getBytes());
                byte[] subjectDigest = mdSubject.digest();    
                long hSubject = 0;
                for (int i = 0; i < 4; i++) 
                {
                        hSubject <<= 8;
                        hSubject |= ((int) subjectDigest[i]) & 0xFF;
                }
                distribute(word,hObject,hSubject,subject,predicate,object,stat1,stat2,stat3,stat4,tripleVal);
                
                
            }
        
            
        }   
    long stopTime1 = System.currentTimeMillis();
System.out.println("Elapsed time was " + (stopTime1 - startTime1) + " miliseconds.");
        reader.close();
       stat1.close();
        stat2.close();
        stat3.close();
        stat4.close();
        conn1.close();
        conn2.close();
        conn3.close();
        conn4.close();
    }
}

