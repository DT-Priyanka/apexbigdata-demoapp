/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demoapp;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private int numTuples = 100;
  private transient int count = 0;
  private boolean failApplication = false;
  private long failureWindowCount = 100;
  private transient long windowId = 0;

  public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
    this.windowId++;
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
      out.emit(Math.random());
    }
    if (failApplication && windowId == failureWindowCount) {
      int res = count / 0;
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * 
   * @param numTuples
   *          number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }

  public boolean getFailApplication()
  {
    return failApplication;
  }

  public void setFailApplication(boolean failApplication)
  {
    this.failApplication = failApplication;
  }

  public long getFailureWindowCount()
  {
    return failureWindowCount;
  }

  public void setFailureWindowCount(long failureWindowCount)
  {
    this.failureWindowCount = failureWindowCount;
  }

}
