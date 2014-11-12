package jp.ac.titech.ylab.drdf;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.MessageDigest;
import java.sql.*;
import java.util.HashMap;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class RDFHashPartitionInsert
{
	@Option(name="-h")
	private boolean showHelp=false;
	
	@Option(name="-d", usage="driver name", required=false)
	private String driverName = "org.h2.Driver";
	
	@Option(name="-c1", usage="first connection name", required=false)
	private String connectionName1 = "jdbc:h2:~/rdfHashValue1";
	
	@Option(name="-c2", usage="second connection name", required=false)
	private String connectionName2 = "jdbc:h2:~/rdfHashValue2";
	
	@Option(name="-c3", usage="third connection name", required=false)
	private String connectionName3 = "jdbc:h2:~/rdfHashValue3";
	
	@Option(name="-f", usage ="file name", required=false)
	private String filename = "D:/sp2b/bin/sp2bnoprefix.n3";
	
	public static HashMap<String, Long> hashvalues = new HashMap<String, Long>();
	public static HashMap<String, Long> hashvaluesSubject = new HashMap<String, Long>();
	static int numberOfMachines = 3;
	public static void distribute(String triple,Long hashOb,Long hashSub,String subject,String predicate,String object,Statement stat1,Statement stat2,Statement stat3) throws Exception
	{
		long hashValueOb = hashOb % numberOfMachines;
		long finalhashValueOb = (hashValueOb<0 ? -hashValueOb : hashValueOb);
		long hashValueSub = hashSub % numberOfMachines;
		long finalhashValueSub = (hashValueSub<0 ? -hashValueSub : hashValueSub);
		String insertHashObjectValue1 = "INSERT INTO rdfHashedbyObjectValue1 VALUES ( "
       		 +"'" +subject +"', " 
       		  +"'" +predicate +"', " 
       		 +"'" +object +"')";
		String insertHashObjectValue2 = "INSERT INTO rdfHashedbyObjectValue2 VALUES ( "
	       		 +"'" +subject +"', " 
	       		  +"'" +predicate +"', " 
	       		 +"'" +object +"')";
		String insertHashObjectValue3 = "INSERT INTO rdfHashedbyObjectValue3 VALUES ( "
	       		 +"'" +subject +"', " 
	       		  +"'" +predicate +"', " 
	       		 +"'" +object +"')";
		String insertHashSubjectValue1 = "INSERT INTO rdfHashedbySubjectValue1 VALUES ( "
	       		 +"'" +subject +"', " 
	       		  +"'" +predicate +"', " 
	       		 +"'" +object +"')";
		String insertHashSubjectValue2 = "INSERT INTO rdfHashedbySubjectValue2 VALUES ( "
	       		 +"'" +subject +"', " 
	       		  +"'" +predicate +"', " 
	       		 +"'" +object +"')";
		String insertHashSubjectValue3 = "INSERT INTO rdfHashedbySubjectValue3 VALUES ( "
	       		 +"'" +subject +"', " 
	       		  +"'" +predicate +"', " 
	       		 +"'" +object +"')";
		
		/*put all triples with hashofobject=1 to table1, similarly, with hashvalue 2 to 
		table2 and hash value 3 to table 3*/
		if (finalhashValueOb == 1)
			 stat1.execute(insertHashObjectValue1);
		else if(finalhashValueOb == 2)
			 stat2.execute(insertHashObjectValue2);
		else
			 stat3.execute(insertHashObjectValue3);
		/*put all triples with hashofsubject=1 to table1, similarly, with hashvalue 2 to 
		table2 and hash value 3 to table 3*/
		if (finalhashValueSub == 1)
			 stat1.execute(insertHashSubjectValue1);
		else if(finalhashValueSub == 2)
			 stat2.execute(insertHashSubjectValue2);
		else
			 stat3.execute(insertHashSubjectValue3);

	}
	

	public static void main(String[] args) throws Exception {
		
		RDFHashPartitionInsert app = new RDFHashPartitionInsert();
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
	public void run() throws Exception{
		//BufferedReader reader = new BufferedReader(new FileReader(new File("D:/sp2b/bin/sp2b.n3")));
		BufferedReader reader = new BufferedReader(new FileReader(new File(
				filename)));
		String inputLine = null;
		Class.forName(driverName);
        Connection conn1 = DriverManager.
            getConnection(connectionName1);
        Statement stat1 = conn1.createStatement();
        Connection conn2 = DriverManager.
                getConnection(connectionName2);
            Statement stat2 = conn2.createStatement();
            Connection conn3 = DriverManager.
                    getConnection(connectionName3);
                Statement stat3 = conn3.createStatement();
       stat1.execute("create table rdfHashedbySubjectValue1(subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat1.execute("create table rdfHashedbyObjectValue1(subject varchar(255), predicate varchar(255),object varchar(5000))");
       
       stat2.execute("create table rdfHashedbySubjectValue2(subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat2.execute("create table rdfHashedbyObjectValue2(subject varchar(255), predicate varchar(255),object varchar(5000))");
       
       stat3.execute("create table rdfHashedbySubjectValue3(subject varchar(255), predicate varchar(255),object varchar(5000))");
       stat3.execute("create table rdfHashedbyObjectValue3(subject varchar(255), predicate varchar(255),object varchar(5000))");
       
		while((inputLine = reader.readLine()) != null)
		{
			String[] triples = inputLine.split("\\n");
			//iterate through string array
			for(String word : triples)
			{
				int k = word.indexOf(" ");
				String subject =word.substring(0,k);
				String predicateandobj = word.substring(k+1);
				int k1 = predicateandobj.indexOf(" ");
				String predicate = predicateandobj.substring(0,k1);
				String ob = predicateandobj.substring(k1+1);
				String object = ob.substring(0,ob.length()-1);
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
				distribute(word,hObject,hSubject,subject,predicate,object,stat1,stat2,stat3);
				
				
			}
		
			
		}	
		reader.close();

	}
}



