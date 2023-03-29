package org.example;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.*;
public class TFReducer extends  Reducer<Text, MapWritable, Text, Text> {
    @Override
    public void reduce(Text Key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException{

        Configuration configuration = context.getConfiguration();
        String fileLocation = context.getConfiguration().get("filepath");
        //System.out.println("fileLocation is : " + fileLocation);
        File file = new File(fileLocation);
        BufferedReader br = new BufferedReader(new FileReader(file));
        HashMap<String, Integer> map = new HashMap<>();
        String str = br.readLine();
        while(str != null) {
            StringTokenizer st = new StringTokenizer(str);
            map.put(st.nextToken(), Integer.parseInt(st.nextToken()));
            str = br.readLine();
        }

        //System.out.println(map);

        PorterStemmer porterStemmer = new PorterStemmer();
        for (MapWritable value : values){
            for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                String newString = porterStemmer.stem(((Text)e.getKey()).toString());
                int dfScore = map.getOrDefault(newString, 0);
                int tfScore = ((IntWritable)e.getValue()).get();
                double score = calcScore(tfScore, dfScore);
                context.write(Key, new Text("" + ((Text)e.getKey()).toString() + "\t" + score));
            }

        }
    }

    private static double calcScore(int tf, int df) {
        return (double)tf * Math.log((10000/(double)df) + 1);
    }
}
