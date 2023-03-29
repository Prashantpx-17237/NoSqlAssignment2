package org.example;

import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.*;
import java.util.*;
public class TFMapper extends Mapper<Object, Text, Text, MapWritable>{

    public static Long getID(String str){
        return Long.parseLong(str.split(".txt")[0]);
    }

    private MapWritable mp  = new MapWritable();
    private final static IntWritable one = new IntWritable(1);
    @Override
    public void map(Object Key, Text value, Context context) throws IOException, InterruptedException {

        String fileLocation = context.getConfiguration().get("filepath");
        File file = new File(fileLocation);
        BufferedReader br = new BufferedReader(new FileReader(file));
        HashMap<String, Integer> map = new HashMap<>();
        String str = br.readLine();
        while(str != null) {
            StringTokenizer st = new StringTokenizer(str);
            map.put(st.nextToken(), Integer.parseInt(st.nextToken()));
            str = br.readLine();
        }

        String[] tokens = value.toString().split("[^\\w']+");

        PorterStemmer porterStemmer = new PorterStemmer();
        mp.clear();
        for(int i = 0 ; i < tokens.length ; i++){

            String newString = porterStemmer.stem(tokens[i]);
            if (!map.containsKey(newString)) continue;
            if(mp.containsKey(new Text(tokens[i]))){
                mp.put(new Text(tokens[i]), new IntWritable(((IntWritable)mp.get(new Text(tokens[i]))).get() + 1));
            }else{
                mp.put(new Text(tokens[i]), one);
            }
        }
        Long id = getID(((FileSplit) context.getInputSplit()).getPath().getName());
        context.write(new Text(String.valueOf(id)), mp );
    }
}
