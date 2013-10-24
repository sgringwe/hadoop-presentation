package com.sgringwe.ustrades;

import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.Partitioner;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
    
public class USTradesJob {
  public class Country implements WritableComparable<Country> {

    private String country;
    private Long value;
    
    public Country() { }

    public Country(String country, Long value) {
      this.country = country;
      this.value = value;
    }
    
    @Override
    public String toString() {
      return (new StringBuilder())
          .append('{')
          .append(country)
          .append(',')
          .append(value)
          .append('}')
          .toString();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      country = WritableUtils.readString(in);
      value = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, country);
      out.writeLong(value);
    }

    @Override
    public int compareTo(Country o) {
      // int result = country.compareTo(o.country);
      // if(0 == result) {
      int result = value.compareTo(o.value);
      // }
      return result;
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    public Long getValue() {
      return value;
    }

    public void setValue(Long value) {
      this.value = value;
    }

  }


  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();

      // Don't accept the first column, which are headers
      if(!line.startsWith("Commodity_Code")) {
        String[] result = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        // Column 3 is the country name
        if(result.length > 3) {
          word.set(result[3]);
          // Country country = new Country(word, timestamp);

          // Column 11 is the 'value' column representing how many of that trade occurred.
          if(result.length > 10) {
            output.collect(word, new IntWritable(Integer.parseInt(result[11])));
          }
          else {
            output.collect(word, one);
          }
        }
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

  public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
      super(Country.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      Country k1 = (Country)w1;
      Country k2 = (Country)w2;
      
      // int result = k1.getCountry().compareTo(k2.getCountry());
      // if(0 == result) {
      int result = -1* k1.getValue().compareTo(k2.getValue());
      // }
      return result;
    }
  }

  public class NaturalKeyGroupingComparator extends WritableComparator {
    protected NaturalKeyGroupingComparator() {
      super(Country.class, true);
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
      Country k1 = (Country)w1;
      Country k2 = (Country)w2;
      
      return k1.getCountry().compareTo(k2.getCountry());
    }
  }

  // public class NaturalKeyPartitioner implements Partitioner<Country, DoubleWritable> {

  //   @Override
  //   public int getPartition(Country key, DoubleWritable val, int numPartitions) {
  //     int hash = key.getCountry().hashCode();
  //     int partition = hash % numPartitions;
  //     return partition;
  //   }

  // }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(USTradesJob.class);
    conf.setJobName("ustrades");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    // Sorting by value
    // conf.setPartitionerClass(NaturalKeyPartitioner.class);
    // conf.setOutputValueGroupingComparator(NaturalKeyGroupingComparator.class);
    // conf.setOutputKeyComparatorClass(CompositeKeyComparator.class);

    conf.setMapperClass(Map.class);

    // Comment these out to simploy output <word, 1> for each word in the file.
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}