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

package org.apache.zeppelin;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.openqa.selenium.WebDriver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ZeppelinITUtils {

  public final static Logger LOGGER = LoggerFactory.getLogger(ZeppelinITUtils.class);

  public static void sleep(long millis, boolean logOutput) {
    if (logOutput) {
      LOGGER.info("Starting sleeping for " + (millis / 1000) + " seconds...");
      LOGGER.info("Caller: " + Thread.currentThread().getStackTrace()[2]);
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOGGER.error("Exception in WebDriverManager while getWebDriver ", e);
    }
    if (logOutput) {
      LOGGER.info("Finished.");
    }
  }

  public static void turnOffImplicitWaits(WebDriver driver) {
    driver.manage().timeouts().implicitlyWait(0, TimeUnit.SECONDS);
  }

  public static void turnOnImplicitWaits(WebDriver driver) {
    driver.manage().timeouts().implicitlyWait(AbstractZeppelinIT.MAX_IMPLICIT_WAIT,
        TimeUnit.SECONDS);
  }

  public static ExecuteWatchdog startZeppelin() throws ExecuteException, IOException {
    CommandLine cmdLine = CommandLine.parse("../bin/zeppelin.sh");
    DefaultExecutor executor = new DefaultExecutor();
    ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);
    executor.execute(cmdLine, new ExecuteResultHandler() {
      @Override
      public void onProcessComplete(int i) {
        LOGGER.info("Zeppelin Server completed without errors");
      }

      @Override
      public void onProcessFailed(ExecuteException e) {
        LOGGER.debug("Zeppelin Server process failed", e);
      }
    });
    return watchdog;
  }
}
