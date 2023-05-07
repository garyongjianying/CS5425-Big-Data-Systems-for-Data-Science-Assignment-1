
// Matric Number: A0155664X
// Name: Ong Jian Ying Gary
// TopkCommonWords.java
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // to allow for multiple inputs.
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; // importing FileSplit 
import org.apache.hadoop.util.GenericOptionsParser; // use GenericOptionsParser to allow us to identify the arguments given in the file to consider stopwords.txt file as well.

// Important for reading file from HDFS.
import java.io.BufferedReader;
import java.io.FileReader;
// import java.io.InputStreamReader;
// import org.apache.hadoop.fs.FileSystem;



// TopkCommonWords MapReduce class definition
public class TopkCommonWords {
    

  // MAPPER CLASS
  public static class CommonWordsMapper
      // make changes to the output of the mapper value, which is the text that names the emitted tuple where it came from, '1' for file 1, '2' for file2.
       extends Mapper<Object, Text, Text, Text>{
    

    // private variables can only be accessed within the Class itself, an outside class has no access to these variables.
    private final static IntWritable one = new IntWritable(1); // type of output value, to be one.
    private Text word = new Text(); // type of output key
    private Set<String> stopWords;

    // utilizing setup() method in CommonWordsMapper class to incorporate our stopwords fom stopwords.txt file.
    protected void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration(); // get configurations that we have set in the main method.
      stopWords = new HashSet<String>(); // define our stopwords as a new list variable
      // use conf.get() to get our stopwords_string that we created in main method to use in MAP, then split it with "," and iterate to get all stop words into a list.
      for (String word: conf.get("stopwords").split(",")) {
        stopWords.add(word); // adding each word to the stopWords set.
      }

    }

    // MAP FUNCTION
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      StringTokenizer itr = new StringTokenizer(value.toString(), " \t\n\r\f"); // converts line to string token, add new argument for delimiter. 
      // file1 name:
      String file1 = new String("task1-input1.txt"); 
      // file2 name:
      String file2 = new String("task1-input2.txt");
      
      // get the current input file name being processed by map function by using FileSplit.
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      // We need to keep track of which file a tuple came from, you can consider emitting a tuple like (keyword, filenum), where filenum is '1' or '2' depending on which file it came from.
      // start of while loop to loop through the documents.
      String filenum = new String(""); // let file num be an empty string initially.
      
      // Start iterating through the text file for tokens.
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken(); // store the token as a string so that we do not go to the next word.
        // the current word token needs to be used as a string to check against the stopwords list.
        // If Y, we continue the loop. If N, then we want to set the word in map.
        if (stopWords.contains(token)) {
          continue; // skip current token if the token is within the stopwords list defined.
        } else {
          word.set(token); // used to return the next token one after another from this StringTokenizer.
          // for the rest of the tokens that are NOT in stopwords list, we proceed with our normal job.
          if (filename.equals(file1)) {
            filenum = "1"; // write the integer value of 1 indicating that the word came from input1.
          } else if (filename.equals(file2)) { 
            filenum = "2"; // write the integer value of 2 indicating that the word came from input2.
          }
          context.write(word, new Text(filenum));
        }
      }
    }
  }

  // REDUCER CLASS
  public static class MinCountReducer 
      // make changes to the input of the reducer value received, which is the filenumber that the word came from. '1' for input1, '2' for input2.
      // make changes to the output of the reducer, since the answer.txt file gives the key as the counts (IntWritable), and value as the word (Text).
      extends Reducer<Text,Text,IntWritable,Text> {
    private IntWritable result = new IntWritable();
    // DEFINED 2 Maps required for sorting within the MinCountReducer count, so that entries get added in the reduce() and we can access it in cleanup().
    // Change our outputting of common words, to incorporate sorting in DESCENDING order.
    Map<String, Integer> unSortedMap1 = new HashMap<>(); // Key: String, Value: Integer - required for getting min count in cleanup() function as per instruction. Contains common words from dict1 with its own count.
    Map<String, Integer> unSortedMap2 = new HashMap<>(); // Key: String, Value: Integer - required for getting min count in cleanup() function as per instruction. Contains common words from dict2 with its own count.
    Map<String, Integer> unSortedMapFinal = new HashMap<>(); // final hashmap to contains common keywords and min count.
    // LinkedHashMap used to preserve ordering of elements in which they were inserted.
    LinkedHashMap<String, Integer> reverseSortedMap = new LinkedHashMap<>(); // Key: Integer, Value: String - to contain the final results after sorting.

    // REDUCER FUNCTION
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      

      
      // We need to iterate through all the tuples (key: Text (word), value: Text (filenum)) from the mapper.
      // use a HashMap to keep track of the counts in the reduce function for each word and counts from each file.
      Map<String, Integer> dict1 = new HashMap<String, Integer>();
      Map<String, Integer> dict2 = new HashMap<String, Integer>();

      // get word and total count for each input file in two dictionaries, dict1 & dict2.
      for (Text val: values) {
        // for values of filenum = "1"
        if (val.toString().equals("1") && dict1.get(key.toString()) == null) {
          dict1.put(key.toString(), 1); // initialize first occurrence of word with integer of 1 in dict1.
        } else if (val.toString().equals("1") && dict1.get(key.toString()) != null) {
          dict1.put(key.toString(), dict1.get(key.toString()) + 1); // update value of dictionary for word in dict1
        }

        // for values of filenum = "2"
        if (val.toString().equals("2") && dict2.get(key.toString()) == null) {
          dict2.put(key.toString(), 1); // initialize first occurrence of word with integer of 1 in dict2.
        } else if (val.toString().equals("2") && dict2.get(key.toString()) != null) {
          dict2.put(key.toString(), dict2.get(key.toString()) + 1); // update value of dictionary for word in dict2
        }
      }



      // start finding the common words in both dictionaries, by iterating through the keys of dict1, and checking if they are in dict2.
      for (Map.Entry<String, Integer> entry: dict1.entrySet()){
        String word_in_dict1 = entry.getKey().toString(); // get word in dict1
        // check if the key (word_in_dict1) exists in dict2!
        if (dict2.containsKey(word_in_dict1) == false) {
          // System.out.println(word_in_dict1); // this will show all the words that are in dict1 but not in dict2.
          // In this case, since the word is in dict1, but not in dict2, we will not care about them.
          continue;
        } else if (dict2.containsKey(word_in_dict1)) {
          // since sorting later will require all the keys, it has to be done in the cleanup() of reducer.
          // Instead of emitting the output in reduce(), we will store them into 2 HashMaps to get mincount later in cleanup().
          // store all the common words into unSortedMap1 - key: Integer (counts from dict1), value: String (word_in_dict1 - common word in both dict1 & dict2)
          // unSortedMap1.put(dict1.get(word_in_dict1), word_in_dict1);
          unSortedMap1.put(word_in_dict1, dict1.get(word_in_dict1));
          // store all the common words into unSortedMap2 - key: Integer (counts from dict2), value: String (word_in_dict1 - common word in both dict1 & dict2)
          // unSortedMap2.put(dict2.get(word_in_dict1), word_in_dict1);
          unSortedMap2.put(word_in_dict1, dict2.get(word_in_dict1));
          // need to change this part first, dont emit the min count here.
          // calculate the minimum counts of occurrences between dict1 and dict2 for common word.
          // Integer min_count2 = Math.min(dict1.get(word_in_dict1), dict2.get(word_in_dict1));
          // // NEED TO EDIT BELOW PART, because we want to incorporate sorting now.
          // // set the result as the minimum count - IntWritable.
          // result.set(min_count2);
          // // write the result directly to context!
          // context.write(result, new Text(word_in_dict1));
        }
      }
    }

    // utilizing cleanup() method in MinCountReducer class to get minimum count for each keyword in unSortedMap1 & unSortedMap2, sort, and emit the top 20 entries.
    protected void cleanup(Context context) throws IOException, InterruptedException {
            // cleanup() method is a way for you to do something after your reduce tasks.

      // Iterate through unSortedMap1 and get the min count and store the result in the unSortedMapFinal map.
      for (Map.Entry<String, Integer> entry: unSortedMap1.entrySet()) {
        // First check if the word being looked at is exactly the same:
        Integer min_count = Math.min(entry.getValue(), unSortedMap2.get(entry.getKey()));
        unSortedMapFinal.put(entry.getKey(), min_count);
      }

      // Use the words as the keys so that we can carry out the sorting as required. We compare by values first.
      unSortedMapFinal.entrySet()
      .stream()
      .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
      .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));

      // define counter variable to keep track of the total number of emitted outputs to be 0 first.
      int counter = 0;
      for (Map.Entry<String, Integer> entry: reverseSortedMap.entrySet()) {
        // check for counter variable to be < 20
        if (counter < 20) {
          // for loop will run as long as counter is < 20. Once counter = 2, we will stop emitting outputs in reducer. Since we start at counter = 0.
          // set the result
          result.set(entry.getValue());
          context.write(result, new Text(entry.getKey()));
          // System.out.println(entry.getKey().toString() + "," + entry.getValue());
        }
        counter = counter + 1; // increment counter for every iteration.
      }
      }
  }
      




    // Driver Program to be run in TopkCommonWords
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
      // edit this part to expect 4 arguments from user for the program to work correctly.
      if (otherArgs.length != 4) {
        System.err.println("Usage: TopkCommonWords <input1> <input2> <input3> <output>");
        System.exit(2);
      }


      // incorporate the stopwords.txt file inside our code now within the main method.
      // introduces the use of setup() and cleanup() methods.
      // setup -> map -> cleanup. setup -> reduce -> cleanup.
      // 1. read the stopwords into a string in the main method
      
      String stopwords_path = new String(args[2]); // getting the stopwords path from args[2] as a string.
      BufferedReader br = new BufferedReader(new FileReader(stopwords_path));

      // Declaring a StringBuilder and String variable to be used in while loop
      StringBuilder sb = new StringBuilder();
      String st;
      // Condition holds true as long as there is a character in string.
      while ((st= br.readLine()) != null) {
        // add read word line by line into our stopwords ArrayList
        sb.append(st); // use StringBuilder to append to list.
        sb.append(","); // append a comma as a delimiter
      }

      String stopwords_string = sb.toString(); // getting the stopwords string from StringBuilder
      // With our stopwords list, we can use conf.set() to set the variable in configurations, with title 'stopwords'.
      conf.set("stopwords", stopwords_string);
      // 2. pass the string to the mappers using conf.set() and conf.get() in our Configurations object with setup() method.


 
      


      // creating a new job with name as 'TopkCommonWords' here
      Job job = Job.getInstance(conf, "TopkCommonWords");
      // changing our code to TopkCommonWords.class instead of WordCount.class
      job.setJarByClass(TopkCommonWords.class);
      // changing our code to CommonWordsMapper.class instead of TokenizerMapper.class
      job.setMapperClass(CommonWordsMapper.class);
      // changing our code to MinCountReducer.class instead of IntSumReducer.class
      job.setReducerClass(MinCountReducer.class);
      // Since our mapper emits different types than the reducer, we can set the types emitted by the mapper, as mentioned by prof.
      // setting mapper output key type
      job.setMapOutputKeyClass(Text.class);
      // setting mapper output value type
      job.setMapOutputValueClass(Text.class);
      // setting reducer output key type
      job.setOutputKeyClass(IntWritable.class);
      // setting reducer output value type
      job.setOutputValueClass(Text.class);

      // HADOOP ALLOWS FOR MULTIPLE INPUT PATHS, so we can just specify the changes here.
      FileInputFormat.addInputPath(job, new Path(args[0])); // set the HDFS path of the input data for task1-input1.txt
      FileInputFormat.addInputPath(job, new Path(args[1])); // set the HDFS path of the input data for task1-input2.txt
      FileInputFormat.addInputPath(job, new Path(args[2])); // set the HDFS path of the input data for stopwords.txt

      // set the HDFS path of the output data, in this case, we are putting the output at the fourth argument.
      FileOutputFormat.setOutputPath(job, new Path(args[3]));
      // Wait till job completion
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 


}



