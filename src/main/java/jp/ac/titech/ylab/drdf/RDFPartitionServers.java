/*
 * Implementing just a logic for deciding the server with respect to hash value of 
 * subject and object
 */
package jp.ac.titech.ylab.drdf;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.security.MessageDigest;
import java.util.*;

public class RDFPartitionServers 
{
		public static void distribute(String triples,Long hashObject,Long hashSubject,String subject,String predicate,String object){
		List<String> Servers = Arrays.asList("Server1", "Server2", "Server3");
		int serverNum = Servers.size();
		long hashValueSubject = hashSubject%serverNum;
		long hashValueObject = hashObject%serverNum;
		System.out.println("triple is"+triples);
		System.out.println("destination server of triple based hashed on subject hash value "+hashValueSubject+" is "+Servers.get((int)hashValueSubject));
		System.out.println("destination server of triple based hashed on object hash value " +hashValueObject+" is "+Servers.get((int)hashValueObject));
		System.out.println("--------------------------------------------------------------");
	}

	public static void main(String[] args) throws Exception
	{


		BufferedReader reader = new BufferedReader(new FileReader(new File("C:/Users/Ajay T P/Desktop/input3.txt")));
		String inputLine = null;
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

				distribute(word,hObject,hSubject,subject,predicate,object);

			}

		}
		reader.close();
	}
}

