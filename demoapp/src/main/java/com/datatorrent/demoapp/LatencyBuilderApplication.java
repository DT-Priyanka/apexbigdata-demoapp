/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demoapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "LatencyBuilder")
public class LatencyBuilderApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);
    randomGenerator.setNumTuples(1000);

    NumberCounter counter = dag.addOperator("counter", NumberCounter.class);
    counter.setGenerateLatency(true);

    dag.addStream("randomData", randomGenerator.out, counter.in);
  }
}
