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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.safari.SafariDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WebDriverManager {

  public final static Logger LOG = LoggerFactory.getLogger(WebDriverManager.class);

  public static WebDriver getWebDriver() {
    WebDriver driver = null;

    if (driver == null) {
      try {
        ChromeOptions options = new ChromeOptions();
        //options.setHeadless(true);
        options.addArguments("window-size=1920x1080");
        driver = new ChromeDriver(options);
      } catch (Exception e) {
        LOG.error("Exception in WebDriverManager while ChromeDriver ", e);
      }
    }

    if (driver == null) {
      try {
        driver = new SafariDriver();
      } catch (Exception e) {
        LOG.error("Exception in WebDriverManager while SafariDriver ", e);
      }
    }

    long start = System.currentTimeMillis();
    String url;
    if (System.getenv("url") != null) {
      url = System.getenv("url");
    } else {
      url = "http://localhost:8080";
      // Wait 10 seconds for the Zeppelin server to open the port
      while (System.currentTimeMillis() - start < 10 * 1000 ) {
        try (Socket ignored = new Socket("localhost", 8080)) {
          break;
        } catch (IOException ignored) {
          ZeppelinITUtils.sleep(100, false);
        }
      }
    }

    boolean loaded = false;

    driver.manage().timeouts().implicitlyWait(AbstractZeppelinIT.MAX_IMPLICIT_WAIT, TimeUnit.SECONDS);
    driver.get(url);

    while (System.currentTimeMillis() - start < 60 * 1000) {
      // wait for page load
      try {
        (new WebDriverWait(driver, 10)).until(new ExpectedCondition<Boolean>() {
          @Override
          public Boolean apply(WebDriver d) {
            return d.findElement(By.xpath("//i[@uib-tooltip='WebSocket Connected']"))
                .isDisplayed();
          }
        });
        loaded = true;
        break;
      } catch (TimeoutException e) {
        LOG.info("Exception in WebDriverManager while WebDriverWait ", e);
        driver.navigate().to(url);
      }
    }

    if (loaded == false) {
      fail();
    }

    driver.manage().window().maximize();
    return driver;
  }
}
