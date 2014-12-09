package com.renren.hadoop.oiv.tools;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {

  /**
   * @param args
   * @throws InvalidTopologyException
   * @throws AlreadyAliveException
   */

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    System.out.println("driver started ...");
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("move", MoveData.class, "move data from fromPath to toPath");
      pgd.driver(args);
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.out.println("drive stoped ...");
    System.exit(exitCode);
  }

}
