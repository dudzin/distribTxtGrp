package pl.pw.elka.distmatrix;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pl.pw.elka.commons.HDFSFileReader;

public class DistMatrixMapper extends 
Mapper<Text, Text, Text, Text> {

	private Text word = new Text();
	private Stemmer stemmer;
	private ForbWords forbWords;
	private String regexForm;
	private HDFSFileReader fileReader;
		
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		initConf(context);	
		List<String> words = initWords(value);
		
		for (String w : words) {		
			w = processWord(w);
			
	    	if(w != null && !w.equals("")){
		        word.set(w);
		        context.write(new Text(key.toString()), word);
	    	} 	
		}
	}
	
	private void initConf(Context context){
		
		fileReader = new HDFSFileReader();
		
		String stemmerClass = context.getConfiguration().get("distMatrixMapper.stemmer") ;
		if(stemmerClass== null) {
		}else if(stemmerClass.equals("Porter")){
			stemmer = new PorterStemmer();
		} 
		
		String forbWordsFile = context.getConfiguration().get("distMatrixMapper.forbWords") ;
		if(forbWordsFile!= null) {
			forbWords = new ForbWords();
			forbWords.setWords(fileReader.readFromFile(forbWordsFile, true));
		} 
		
		String regexFormFile = context.getConfiguration().get("distMatrixMapper.regexForm") ;
		if(regexFormFile!= null) {
			regexForm = fileReader.readFromFile(regexFormFile, false).get(0);
		} 
		
	}
	
	private List<String> initWords(Text value){
		
		String[] words = value.toString().split(" ");
		List<String> list = new ArrayList<String>();

	    for(String s : words) {
	    	s= s.replaceAll(regexForm,"");
	       if(s != null && s.length() > 0) {
	          list.add(s);
	       }
	    }
		
		return list;
	}
	
	private String processWord(String w){
		
		if(stemmer != null){
	        stemmer.add(w);
	    	stemmer.stem();
	    	w =stemmer.toString();
    	}
    	if(forbWords != null){
    		if(forbWords.isWorb(w)){
    			w = null;
    		}
    	}
    	return w;
	}

}
