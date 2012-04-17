package com.twitter.twadoop.pig.stats;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.tools.pigstats.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Hacked up code for how we can gather stats as jobs kick off and expose them in a web UI
 */
public class PigStatsDataVizCollector implements PigProgressNotificationListener {
  protected Log LOG = LogFactory.getLog(getClass());

  private List<JobInfo> jobInfoList = new ArrayList<JobInfo>();

  private String scriptFingerprint;

  public PigStatsDataVizCollector() { }

  @Override
  public void initialPlanNotification(MROperPlan plan) {
    Map<OperatorKey, MapReduceOper>  planKeys = plan.getKeys();
    for (Map.Entry<OperatorKey, MapReduceOper> entry : planKeys.entrySet()) {
      OperatorKey operatorKey = entry.getKey();
      MapReduceOper mapReduceOper = entry.getValue();
      String scope = operatorKey.toString();
      String alias = ScriptState.get().getAlias(mapReduceOper);
      String feature = ScriptState.get().getPigFeature(mapReduceOper);

      // this shows how we can get the basic info about all nameless jobs before any execute.
      // we can traverse the plan to build a DAG of this info
      LOG.info("initialPlanNotification: alias: " + alias + ", scope: " + scope + ", feature: " + feature);
    }
  }

  @Override
  public void jobStartedNotification(String scriptId, String assignedJobId) {
    PigStats.JobGraph jobGraph = PigStats.get().getJobGraph();
      LOG.info("jobStartedNotification - jobId " + assignedJobId + ", jobGraph:\n" + jobGraph);

    // this verifies that later when a job gets kicked off we can associate the jobId with the scope
    for (JobStats jobStats : jobGraph) {
      if (assignedJobId.equals(jobStats.getJobId())) {
        LOG.info("jobStartedNotification - scope " + jobStats.getName() + " is jobId " + assignedJobId);
      }
    }
  }


  /* The code below is all borrowed from my stats collection work. We'd change it to our needs */

  @Override
  public void jobFailedNotification(String scriptId, JobStats stats) {
    collectStats(scriptId, stats);
  }

  @Override
  public void jobFinishedNotification(String scriptId, JobStats stats) {
    collectStats(scriptId, stats);
  }

  @Override
  public void launchCompletedNotification(String scriptId, int numJobsSucceeded) {
    if (scriptFingerprint == null) {
      LOG.warn("scriptFingerprint not set for this script - not saving stats." );
      return;
    }

    ScriptStats scriptStats = new ScriptStats(scriptId, scriptFingerprint, jobInfoList);

    try {
      outputStatsData(scriptStats);
    } catch (IOException e) {
      LOG.error("Exception outputting scriptStats", e);
    }
  }

  public void outputStatsData(ScriptStats scriptStats) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("Collected stats for script:\n" + ScriptStats.toJSON(scriptStats));
    }
  }

  /**
   * Collects statistics from JobStats and builds a nested Map of values. Subsclass ond override
   * if you'd like to generate different stats.
   *
   * @param scriptId
   * @param stats
   * @return
   */
  protected void collectStats(String scriptId, JobStats stats) {

    // put the job conf into a Properties object so we can serialize them
    Properties jobConfProperties = new Properties();
    if (stats.getInputs() != null && stats.getInputs().size() > 0 &&
      stats.getInputs().get(0).getConf() != null) {

      Configuration conf = stats.getInputs().get(0).getConf();
      for (Map.Entry<String, String> entry : conf) {
        jobConfProperties.setProperty(entry.getKey(), entry.getValue());
      }

      if (scriptFingerprint == null)  {
        scriptFingerprint = conf.get("pig.logical.plan.signature");
      }
    }

    jobInfoList.add(new JobInfo(stats, jobConfProperties));
  }

  @Override
  public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) { }

  @Override
  public void launchStartedNotification(String scriptId, int numJobsToLaunch) { }

  @Override
  public void outputCompletedNotification(String scriptId, OutputStats outputStats) { }

  @Override
  public void progressUpdatedNotification(String scriptId, int progress) { }

  private static Properties filterProperties(Properties input, String... prefixes) {
    Properties filtered = new Properties();
    for(String key : input.stringPropertyNames()) {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          filtered.setProperty(key, input.get(key).toString());
          break;
        }
      }
    }
    return filtered;
  }
}