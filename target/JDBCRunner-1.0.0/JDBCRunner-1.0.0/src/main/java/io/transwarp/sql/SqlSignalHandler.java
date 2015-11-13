package io.transwarp.sql;

import sun.misc.Signal;
import sun.misc.SignalHandler;

public class SqlSignalHandler implements SignalHandler {
  private TestSQL runner;
  
  public SqlSignalHandler (TestSQL runner) {
    this.runner = runner;
  }
  
  public void handle(Signal signal) {
    System.out.println("Signal handler called for signal " + signal);
    runner.close();
  }

}
