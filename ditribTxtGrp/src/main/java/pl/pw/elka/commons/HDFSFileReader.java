package pl.pw.elka.commons;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSFileReader {

	public ArrayList<String> readFromFile(String path, boolean trim){
		ArrayList<String> lines =  new ArrayList<String>();;
		try{
			
	        FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(path))));
	        String line;
	        line=br.readLine();
	        while (line != null){
	        	if(trim){
	        		lines.add(line.trim());
	        	}else {
	                lines.add(line);
	        	}
	                line=br.readLine();
	        }
		}catch(Exception e){
			
		}
		return lines;
	}
}
