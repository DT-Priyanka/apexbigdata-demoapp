package com.datatorrent.demoapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "BlockedOperatorApplication")
public class BlockedOperatorApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);

    NumberCounter counter = dag.addOperator("counter", NumberCounter.class);
    counter.setBlockOperator(true);

    dag.addStream("randomData", randomGenerator.out, counter.in);

  }

}
