/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.scheduler;

import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreter;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.util.ExecutorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RemoteScheduler runs in ZeppelinServer and proxies Scheduler running on RemoteInterpreter.
 * It is some kind of FIFOScheduler, but only run the next job after the current job is submitted
 * to remote.
 */
public class RemoteScheduler extends AbstractScheduler {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteScheduler.class);

  private final RemoteInterpreter remoteInterpreter;
  private final ExecutorService executor;

  public RemoteScheduler(String name,
                         ExecutorService executor,
                         RemoteInterpreter remoteInterpreter) {
    super(name);
    this.executor = executor;
    this.remoteInterpreter = remoteInterpreter;
  }

  /**
   * This is the logic of running job.
   * Subclass can use this method and can customize where and when to run this method.
   *
   * @param runningJob
   */
  @Override
  protected void runJob(Job<?> runningJob) {
    LOGGER.info("Begin job {}", runningJob.getId());
    if (runningJob.isAborted() || runningJob.getStatus().equals(Job.Status.ABORT)) {
      LOGGER.info("Job {} is aborted", runningJob.getId());
      runningJob.setStatus(Job.Status.ABORT);
      runningJob.aborted = false;
      return;
    }

    LOGGER.info("Job {} started by scheduler {}", runningJob.getId(), name);
    runningJob.run();
    Object jobResult = runningJob.getReturn();
    LOGGER.info("Jobresult {} for {}", jobResult, runningJob.getId());
    synchronized (runningJob) {
      if (runningJob.isAborted()) {
        LOGGER.debug("Job Aborted, {}, {}", runningJob.getId(), runningJob.getErrorMessage());
        runningJob.setStatus(Job.Status.ABORT);
      } else if (runningJob.getException() != null || (jobResult instanceof InterpreterResult
          && ((InterpreterResult) jobResult).code() == InterpreterResult.Code.ERROR)) {
        LOGGER.debug("Job Error, {}, {}", runningJob.getId(), runningJob.getReturn());
        runningJob.setStatus(Job.Status.ERROR);
      } else {
        LOGGER.debug("Job Finished, {}, Result: {}", runningJob.getId(), runningJob.getReturn());
        runningJob.setStatus(Job.Status.FINISHED);
      }
    }
    LOGGER.info("Job {} finished by scheduler {} with status {}", runningJob.getId(), name,
        runningJob.getStatus());
    // reset aborted flag to allow retry
    runningJob.aborted = false;
    jobs.remove(runningJob.getId());
  }

  @Override
  public void runJobInScheduler(Job<?> job) {
    JobRunner jobRunner = new JobRunner(this, job);
    executor.execute(jobRunner);
    String executionMode =
        remoteInterpreter.getProperty(".execution.mode", "paragraph");
    if (executionMode.equals("paragraph")) {
      // wait until it is submitted to the remote
      while (!jobRunner.getJobSubmittedInRemote().get()
          && !Thread.currentThread().isInterrupted()) {
        try {
          LOGGER.info("Job {} not submitted", job.getId());
          synchronized (jobRunner.getJobSubmittedInRemote()) {
            jobRunner.getJobSubmittedInRemote().wait(10000);
          }
        } catch (InterruptedException e) {
          LOGGER.error("Exception in RemoteScheduler while jobRunner.isJobSubmittedInRemote " +
              "queue.wait", e);
          // Restore interrupted state...
          Thread.currentThread().interrupt();
        }
      }
      LOGGER.info("Job {} submitted", job.getId());
    } else if (executionMode.equals("note")) {
      // wait until it is finished
      while (!jobRunner.getJobExecuted().get() && !Thread.currentThread().isInterrupted()) {
        try {
          LOGGER.info("Job {} not executed", job.getId());
          synchronized (jobRunner.getJobExecuted()) {
            jobRunner.getJobExecuted().wait(10000);
          }
        } catch (InterruptedException e) {
          LOGGER.error("Exception in RemoteScheduler while jobRunner.isJobExecuted " +
              "queue.wait", e);
          // Restore interrupted state...
          Thread.currentThread().interrupt();
        }
      }
      LOGGER.info("Job {} executed", job.getId());
    } else {
      throw new RuntimeException("Invalid job execution.mode: " + executionMode +
          ", only 'note' and 'paragraph' are valid");
    }
  }

  /**
   * Role of the class is getting status info from remote process from PENDING to
   * RUNNING status. This thread will exist after job is in RUNNING/FINISHED state.
   */
  private class JobStatusPoller implements Runnable {
    private final JobListener listener;
    private final Job<?> job;
    private Status lastStatus;

    public JobStatusPoller(Job<?> job,
        JobListener listener) {
      this.job = job;
      this.listener = listener;
      lastStatus = Status.UNKNOWN;
    }

    @Override
    public void run() {
      updateStatus();
    }

    public void updateStatus() {
      if (!remoteInterpreter.isOpened()) {
        return;
      }
      Status status = Status.valueOf(remoteInterpreter.getStatus(job.getId()));
      if (status == Status.UNKNOWN) {
        // not found this job in the remote schedulers.
        // maybe not submitted, maybe already finished
        return;
      }
      LOGGER.info("Status {} for {}", status, job.getId());
      if (!lastStatus.equals(status)) {
        listener.onStatusChange(job, lastStatus, status);
        lastStatus = status;
      }
    }
  }

  private class JobRunner implements Runnable, JobListener {
    private final Logger LOGGER = LoggerFactory.getLogger(JobRunner.class);
    private final RemoteScheduler scheduler;
    private final Job<?> job;
    private final AtomicBoolean jobSubmittedRemotely;
    private final AtomicBoolean jobExecuted;

    private final ScheduledExecutorService jobStatusPoller;

    public JobRunner(RemoteScheduler scheduler, Job<?> job) {
      this.jobStatusPoller = Executors
          .newSingleThreadScheduledExecutor(
              new NamedThreadFactory("JobStatusPoller-" + job.getId()));
      this.scheduler = scheduler;
      this.job = job;
      this.jobSubmittedRemotely = new AtomicBoolean(false);
      this.jobExecuted = new AtomicBoolean(false);
    }

    public AtomicBoolean getJobSubmittedInRemote() {
      return jobSubmittedRemotely;
    }

    public AtomicBoolean getJobExecuted() {
      return jobExecuted;
    }

    @Override
    public void run() {
      jobStatusPoller.scheduleAtFixedRate(new JobStatusPoller(job, this), 100, 100,
          TimeUnit.MILLISECONDS);
      LOGGER.info("Running job {}", job.getId());
      scheduler.runJob(job);
      jobExecuted.set(true);
      synchronized (jobExecuted) {
        jobExecuted.notifyAll();
      }
      jobSubmittedRemotely.set(true);
      synchronized (jobSubmittedRemotely) {
        jobSubmittedRemotely.notifyAll();
      }
      LOGGER.info("Shutdown of JobStatusPoller-{}", job.getId());
      ExecutorUtil.softShutdown("JobStatusPoller-" + job.getId(), jobStatusPoller, 2,
          TimeUnit.SECONDS);
    }

    @Override
    public void onProgressUpdate(Job<?> job, int progress) {
    }

    // Call by JobStatusPoller, update status when JobStatusPoller get new status.
    @Override
    public void onStatusChange(Job<?> job, Status before, Status after) {
      LOGGER.info("Status-Change {} from {} to {}", job.getId(), before, after);
      if (jobExecuted.get()) {
        if (after.isCompleted()) {
          // it can be status of last run.
          // so not updating the remoteStatus
          return;
        } else if (after.isRunning()) {
          jobSubmittedRemotely.set(true);
          synchronized (jobSubmittedRemotely) {
            jobSubmittedRemotely.notifyAll();
          }
        }
      } else {
        jobSubmittedRemotely.set(true);
        synchronized (jobSubmittedRemotely) {
          jobSubmittedRemotely.notifyAll();
        }
      }
      job.setStatus(after);
    }
  }

  @Override
  public void stop(int stopTimeoutVal, TimeUnit stopTimeoutUnit) {
    super.stop();
    ExecutorUtil.softShutdown(name, executor, stopTimeoutVal, stopTimeoutUnit);
  }

}
