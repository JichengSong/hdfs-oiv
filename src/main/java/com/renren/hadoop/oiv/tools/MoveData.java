/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.renren.hadoop.oiv.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MoveData {

  public static final String DATA_TO_DIR = "data.to.dir";
  public static final String DEFAULT_DATA_OUTPUT_DIR = "/cool_data";

  public static class MoveDataMapper extends
      Mapper<Object, Text, Text, IntWritable> {

    // private final static IntWritable one = new IntWritable(1);
    // private Text word;
    private FileSystem fs;
    private String dataToDir;
    BlackListManager blackListManager;

    protected void setup(Context context) throws IOException,
        InterruptedException {
      this.fs = FileSystem.get(context.getConfiguration());
      this.dataToDir = context.getConfiguration().get(DATA_TO_DIR,
          DEFAULT_DATA_OUTPUT_DIR);
      this.blackListManager = new BlackListManager(context.getConfiguration());
      // NOTHING
    }

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      // filter illegal file
      if (value.toString().equals("")) {
        return;
      }
      // filter files that in blackList
      if (this.blackListManager.isInBlackList(value.toString())) {
        return;
      }
      // move it
      try {

        Path fromPath = new Path(value.toString());
        // filter directory
        if (this.fs.isDirectory(fromPath)) {
          return;
        }
        //
        Path toPath = new Path(this.dataToDir + value.toString());
        // create toPath
        this.fs.mkdirs(toPath.getParent());
        // move fromPath to toPath
        this.fs.rename(fromPath, toPath);
      } catch (Exception e) {
        e.printStackTrace();
        this.fs = FileSystem.get(context.getConfiguration());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();
    if (otherArgs.length != 3 && otherArgs.length != 4) {
      System.err
          .println("Usage: MoveData <data_from_dir> <data_to_dir> <job_out_dir> [<black_list_file>]");
      System.exit(2);
    }
    String blackListFile = null;
    if (otherArgs.length == 4) {
      blackListFile = otherArgs[3];
    } else {
      System.err.println("Warn: black_list_file param is not given");
    }

    String dataFromDir = otherArgs[0];
    conf.set(DATA_TO_DIR, otherArgs[1]);
    conf.set(BlackListManager.BLACK_LIST_FILE, blackListFile);
    String jobOutDir = otherArgs[2];

    Job job = new Job(conf, "clearData");
    job.setJarByClass(MoveData.class);
    job.setMapperClass(MoveDataMapper.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(dataFromDir));
    FileOutputFormat.setOutputPath(job, new Path(jobOutDir));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
