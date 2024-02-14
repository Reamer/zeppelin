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

package org.apache.zeppelin.livy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.zeppelin.test.helper.DownloadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LivyITPrepare {

  private static final Logger LOGGER = LoggerFactory.getLogger(LivyITPrepare.class);

  public static void main(String[] args) throws IOException {
    LOGGER.info("Running the main method");
    if (args.length == 5) {
      String livyVersion = args[0];
      File livyHome = new File(args[1]);
      String sparkVersion = args[2];
      String sparkHadoopVersion = args[3];
      File sparkHome = new File(args[4]);
      DownloadUtils.downloadLivy(livyVersion, livyHome);
      DownloadUtils.downloadSpark(sparkVersion, sparkHadoopVersion, sparkHome);
    } else {
      printHelp(args);
    }
  }

  private static void printHelp(String[] args) {
    LOGGER.error(
        "Wrong amount of arguments. First argument livyVersion,"
            + " second Livyhome, third SparkVersion, fourth SparkHadoopVersion, fifth SparkHome,"
            + " got {}",
        Arrays.toString(args));
  }
}
