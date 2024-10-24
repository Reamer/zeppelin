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


import com.google.common.base.Function;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.ElementClickInterceptedException;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.FluentWait;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractZeppelinIT {

  protected WebDriverManager manager;

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractZeppelinIT.class);
  protected static final long MIN_IMPLICIT_WAIT = 5;
  protected static final long MAX_IMPLICIT_WAIT = 30;
  protected static final long MAX_BROWSER_TIMEOUT_SEC = 30;
  protected static final long MAX_PARAGRAPH_TIMEOUT_SEC = 120;

  protected void authenticationUser(String userName, String password) {
    pollingWait(
        By.xpath("//div[contains(@class, 'navbar-collapse')]//li//button[contains(.,'Login')]"),
        MAX_BROWSER_TIMEOUT_SEC).click();

    ZeppelinITUtils.sleep(1000, false);

    pollingWait(By.xpath("//*[@id='userName']"), MAX_BROWSER_TIMEOUT_SEC).sendKeys(userName);
    pollingWait(By.xpath("//*[@id='password']"), MAX_BROWSER_TIMEOUT_SEC).sendKeys(password);
    pollingWait(
        By.xpath("//*[@id='loginModalContent']//button[contains(.,'Login')]"),
        MAX_BROWSER_TIMEOUT_SEC).click();

    ZeppelinITUtils.sleep(1000, false);
  }

  protected void logoutUser(String userName) throws URISyntaxException {
    ZeppelinITUtils.sleep(500, false);
    manager.getWebDriver().findElement(
        By.xpath("//div[contains(@class, 'navbar-collapse')]//li[contains(.,'" + userName + "')]")).click();
    ZeppelinITUtils.sleep(500, false);
    manager.getWebDriver().findElement(
        By.xpath("//div[contains(@class, 'navbar-collapse')]//li[contains(.,'" + userName + "')]//a[@ng-click='navbar.logout()']")).click();
    ZeppelinITUtils.sleep(2000, false);
    if (manager.getWebDriver().findElement(
        By.xpath("//*[@id='loginModal']//div[contains(@class, 'modal-header')]/button")).isDisplayed()) {
      manager.getWebDriver().findElement(
          By.xpath("//*[@id='loginModal']//div[contains(@class, 'modal-header')]/button")).click();
    }
    manager.getWebDriver().get(new URI(manager.getWebDriver().getCurrentUrl()).resolve("/#/").toString());
    ZeppelinITUtils.sleep(500, false);
  }
  
  protected void setTextOfParagraph(int paragraphNo, String text) {
    String paragraphXpath = getParagraphXPath(paragraphNo);

    try {
      manager.getWebDriver().manage().timeouts().implicitlyWait(Duration.ofMillis(100));
      // make sure ace code is visible, if not click on show editor icon to make it visible
      manager.getWebDriver()
        .findElement(By.xpath(paragraphXpath + "//span[@class='icon-size-fullscreen']")).click();
    } catch (NoSuchElementException e) {
      // ignore
    } finally {
      manager.getWebDriver().manage().timeouts()
        .implicitlyWait(Duration.ofSeconds(AbstractZeppelinIT.MAX_BROWSER_TIMEOUT_SEC));
    }
    String editorId = pollingWait(By.xpath(paragraphXpath + "//div[contains(@class, 'editor')]"),
        MIN_IMPLICIT_WAIT).getAttribute("id");
    if (manager.getWebDriver() instanceof JavascriptExecutor) {
      ((JavascriptExecutor) manager.getWebDriver())
        .executeScript("ace.edit('" + editorId + "'). setValue('" + text + "')");
    } else {
      throw new IllegalStateException("This driver does not support JavaScript!");
    }
  }

  protected void runParagraph(int paragraphNo) {
    By by = By.xpath(getParagraphXPath(paragraphNo) + "//span[@class='icon-control-play']");
    clickAndWait(by);
  }

  protected void cancelParagraph(int paragraphNo) {
    By by = By.xpath(getParagraphXPath(paragraphNo) + "//span[@class='icon-control-pause']");
    clickAndWait(by);
  }

  protected static String getParagraphXPath(int paragraphNo) {
    return "(//div[@ng-controller=\"ParagraphCtrl\"])[" + paragraphNo + "]";
  }

  protected static String getNoteFormsXPath() {
    return "(//div[@id='noteForms'])";
  }

  protected boolean waitForParagraph(final int paragraphNo, final String state) {
    By locator = By.xpath(getParagraphXPath(paragraphNo)
        + "//div[contains(@class, 'control')]//span[2][contains(.,'" + state + "')]");
    WebElement element = pollingWait(locator, MAX_PARAGRAPH_TIMEOUT_SEC);
    return element.isDisplayed();
  }

  protected String getParagraphStatus(final int paragraphNo) {
    By locator = By.xpath(getParagraphXPath(paragraphNo)
        + "//div[contains(@class, 'control')]/span[2]");

    return manager.getWebDriver().findElement(locator).getText();
  }

  protected boolean waitForText(final String txt, final By locator) {
    try {
      WebElement element = pollingWait(locator, MAX_BROWSER_TIMEOUT_SEC);
      return txt.equals(element.getText());
    } catch (TimeoutException e) {
      return false;
    }
  }

  protected WebElement pollingWait(final By locator, final long timeWait) {
    Wait<WebDriver> wait = new FluentWait<>(manager.getWebDriver())
        .withTimeout(Duration.of(timeWait, ChronoUnit.SECONDS))
        .pollingEvery(Duration.of(1, ChronoUnit.SECONDS))
        .ignoring(NoSuchElementException.class);

    return wait.until((Function<WebDriver, WebElement>) driver -> driver.findElement(locator));
  }

  protected void createNewNote() {
    clickAndWait(By.xpath("//div[contains(@class, \"col-md-4\")]/div/h5/a[contains(.,'Create new" +
        " note')]"));

    WebDriverWait block =
      new WebDriverWait(manager.getWebDriver(), Duration.ofSeconds(MAX_BROWSER_TIMEOUT_SEC));
    block.until(ExpectedConditions.visibilityOfElementLocated(By.id("noteCreateModal")));
    clickAndWait(By.id("createNoteButton"));
    block.until(ExpectedConditions.invisibilityOfElementLocated(By.id("createNoteButton")));
  }

  protected void deleteTestNotebook(final WebDriver driver) {
    WebDriverWait block = new WebDriverWait(driver, Duration.ofSeconds(MAX_BROWSER_TIMEOUT_SEC));
    driver.findElement(By.xpath(".//*[@id='main']//button[@ng-click='moveNoteToTrash(note.id)']"))
        .sendKeys(Keys.ENTER);
    block.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='main']//button[@ng-click='moveNoteToTrash(note.id)']")));
    driver.findElement(By.xpath("//div[@class='modal-dialog'][contains(.,'This note will be moved to trash')]" +
        "//div[@class='modal-footer']//button[contains(.,'OK')]")).click();
    ZeppelinITUtils.sleep(100, false);
  }

  protected void deleteTrashNotebook(final WebDriver driver) {
    WebDriverWait block = new WebDriverWait(driver, Duration.ofSeconds(MAX_BROWSER_TIMEOUT_SEC));
    driver.findElement(By.xpath(".//*[@id='main']//button[@ng-click='removeNote(note.id)']"))
        .sendKeys(Keys.ENTER);
    block.until(ExpectedConditions.visibilityOfElementLocated(By.xpath(".//*[@id='main']//button[@ng-click='removeNote(note.id)']")));
    driver.findElement(By.xpath("//div[@class='modal-dialog'][contains(.,'This cannot be undone. Are you sure?')]" +
        "//div[@class='modal-footer']//button[contains(.,'OK')]")).click();
    ZeppelinITUtils.sleep(100, false);
  }

  protected void clickAndWait(final By locator) {
    WebElement element = pollingWait(locator, MAX_IMPLICIT_WAIT);
    try {
      element.click();
      ZeppelinITUtils.sleep(1000, false);
    } catch (ElementClickInterceptedException e) {
      // if the previous click did not happened mean the element is behind another clickable element
      Actions action = new Actions(manager.getWebDriver());
      action.moveToElement(element).click().build().perform();
      ZeppelinITUtils.sleep(1500, false);
    }
  }

  protected void handleException(String message, Exception e) throws Exception {
    LOGGER.error(message, e);
    File scrFile = ((TakesScreenshot) manager.getWebDriver()).getScreenshotAs(OutputType.FILE);
    LOGGER.error("ScreenShot::\ndata:image/png;base64," + new String(Base64.encodeBase64(FileUtils.readFileToByteArray(scrFile))));
    throw e;
  }

}
