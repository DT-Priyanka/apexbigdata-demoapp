package com.datatorrent.demoapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "FailingOperatorApplication")
public class FailingOperatorApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(10000);
    randomGenerator.setFailOperator(true);

    NumberCounter counter = dag.addOperator("counter", NumberCounter.class);

    dag.addStream("randomData", randomGenerator.out, counter.in);

  }

}
