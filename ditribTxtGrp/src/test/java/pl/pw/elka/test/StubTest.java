package pl.pw.elka.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import org.junit.*;

import pl.pw.elka.distmatrix.*;

public class StubTest {

  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable> mapReduceDriver;
  DistMatrixMapper mapper;
  Text testText;
  Text textName;
  ForbWords forbWords;
  
  @Before
  public void setUp() {

	mapper = new DistMatrixMapper();
    //mapper.setStemmer(new PorterStemmer());
    mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
   // mapDriver.setMapper(mapper);

    StubReducer reducer = new StubReducer();
    reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
    reduceDriver.setReducer(reducer);

    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, IntWritable>();
    //mapReduceDriver.setMapper(mapper);
    //mapReduceDriver.setReducer(reducer);
    
	  testText  = new Text("Warsaw University of Technology is the best university in a whole universe");
	  textName  = new Text("test");
	  forbWords = new ForbWords();
	  forbWords.addWord("of");
	  forbWords.addWord("is");
	  forbWords.addWord("the");
	  forbWords.addWord("in");
	  forbWords.addWord("a");
  }

  /*
   * Test the mapper.
   */
  @Test
  public void testPorterStemmer(){
	  Stemmer ps = new PorterStemmer();
	  String testText = "strings";
	 
      ps.add(testText);
      ps.stem();
      assertEquals("string", ps.toString());
      
      testText = "sTRings";
      ps.add(testText);
      ps.stem();
      assertEquals("sTRing", ps.toString());
      
      testText = "university";
      ps.add(testText);
      ps.stem();
      assertEquals("univers", ps.toString());    
  
  }
   
  
  
  /*
   * Test the mapper.
   */
  /*
  @Test
  public void testStemmerMapper() {
	   
	    mapDriver.withInput(textName, testText);
	    mapDriver.withOutput(new Text("Warsaw"), textName);
	    mapDriver.withOutput(new Text("Univers"), textName);
	    mapDriver.withOutput(new Text("of"), textName);
	    mapDriver.withOutput(new Text("Technolog"), textName);
	    mapDriver.withOutput(new Text("is"), textName);
	    mapDriver.withOutput(new Text("the"), textName);
	    mapDriver.withOutput(new Text("best"), textName);
	    mapDriver.withOutput(new Text("univers"), textName);
	    mapDriver.withOutput(new Text("in"), textName);
	    mapDriver.withOutput(new Text("a"), textName);
	    mapDriver.withOutput(new Text("whole"), textName);
	    mapDriver.withOutput(new Text("univers"), textName);
	    try {
			mapDriver.runTest();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

  }  
  
  @Test
  public void testForbWordsMapper() {

	  mapper.setForbWords(forbWords);
	    
	  mapDriver.withInput(textName, testText);
	  mapDriver.withOutput(new Text("Warsaw"), textName);
	  mapDriver.withOutput(new Text("Univers"), textName);
	  mapDriver.withOutput(new Text("Technolog"), textName);
	  mapDriver.withOutput(new Text("best"), textName);
	  mapDriver.withOutput(new Text("univers"), textName);
	  mapDriver.withOutput(new Text("whole"), textName);
	  mapDriver.withOutput(new Text("univers"), textName);
	  try {
		mapDriver.runTest();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

  }

  /*
   * Test the reducer.
   */
  @Test
  public void testReducer() {

	 /* List<IntWritable> values = new ArrayList<IntWritable>();
	    values.add(new IntWritable(1));
	    values.add(new IntWritable(1));
	    reduceDriver.withInput(new Text("cat"), values);
	    reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
	    reduceDriver.runTest();
*/
  }


  /*
   * Test the mapper and reducer working together.
   */
  @Test
  public void testMapReduce() {
/*
	  mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
	    mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
	    mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
	    mapReduceDriver.runTest();
*/
  }
}
