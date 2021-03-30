package org.apache.zeppelin.notebook.scheduler;

import org.apache.zeppelin.notebook.Note;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.quartz.TriggerListener;

public class ZeppelinCronJobTriggerListerner implements TriggerListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZeppelinCronJobTriggerListerner.class);

  @Override
  public String getName() {
    return "ZeppelinCronJobTriggerListerner";
  }

  @Override
  public void triggerFired(Trigger trigger, JobExecutionContext context) {
    // Do nothing
  }

  @Override
  public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
    JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
    Note note = (Note) jobDataMap.get("note");
    if (note.haveRunningOrPendingParagraphs()) {
      LOGGER.warn(
          "execution of the cron job is skipped because there is a running or pending paragraph (note id: {})",
          note.getId());
      return true;
    }
    return false;
  }

  @Override
  public void triggerMisfired(Trigger trigger) {
    // Do nothing
  }

  @Override
  public void triggerComplete(Trigger trigger, JobExecutionContext context,
      CompletedExecutionInstruction triggerInstructionCode) {
    // Do nothing
  }

}
