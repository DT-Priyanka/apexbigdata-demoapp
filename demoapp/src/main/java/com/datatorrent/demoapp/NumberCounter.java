package com.datatorrent.demoapp;

import org.slf4j.Logger;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

public class NumberCounter extends BaseOperator
{
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(NumberCounter.class);
  private Double sum = new Double(0);
  private boolean generateLatency = false;
  private boolean blockOperator = false;
  private long failureWindowCount = 100;
  private transient long windowId = 0;

  public final transient DefaultInputPort<Double> in = new DefaultInputPort<Double>()
  {

    @Override
    public void process(Double number)
    {
      if (blockOperator && windowId > failureWindowCount) {
        while (true) {
          LOG.info("blocking operator.");
        }
      }
      sum += number;

      if (generateLatency) {
        LOG.info("Adding number: " + number + " to sum: " + sum);
        LOG.info("After addition sum: " + sum);
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId++;
    sum = 0d;
  }

  @Override
  public void endWindow()
  {
    if (generateLatency) {
      LOG.info("In end window.");
    }
  }

  public boolean isGenerateLatency()
  {
    return generateLatency;
  }

  public void setGenerateLatency(boolean generateLatency)
  {
    this.generateLatency = generateLatency;
  }

  public boolean isBlockOperator()
  {
    return blockOperator;
  }

  public void setBlockOperator(boolean blockOperator)
  {
    this.blockOperator = blockOperator;
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
