package com.renren.hadoop.oiv.tools;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class BlackListManager {
  public static final String BLACK_LIST_FILE = "black.list.file";

  private Set<String> pathSet = new HashSet<String>();
  private Configuration conf;
  private Log LOG = LogFactory.getLog(this.getClass());

  public BlackListManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.initInternal();
  }

  /**
   * 
   * @param path
   * @return
   */
  public boolean isInBlackList(String path) {
    for (String item : this.pathSet) {
      if (path.startsWith(item)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 从block.list.file指定的文件中读取黑名单
   * 
   * @throws IOException
   */
  private void initInternal() throws IOException {

    String blackFile = this.conf.get(BLACK_LIST_FILE);
    if (blackFile == null || "".equals(blackFile)) {
      this.LOG.warn("BLACK LIST FILE is not set !");
      return;
    }
    FileSystem filesystem = FileSystem.get(conf);
    DataInputStream inStream = filesystem.open(new Path(blackFile));
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));

    String line = null;
    while ((line = reader.readLine()) != null) {
      if ("".equals(line)) {
        continue;
      }
      this.pathSet.add(line);
    }
    reader.close();
  }

  @Override
  public String toString() {
    String result = "BlackListPath:";
    for (String item : this.pathSet) {
      result += item + " ";
    }
    return result.trim();
  }

}
