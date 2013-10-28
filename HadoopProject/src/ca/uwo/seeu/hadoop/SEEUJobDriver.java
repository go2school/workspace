/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwo.seeu.hadoop;

import org.apache.hadoop.util.ProgramDriver;

import ca.uwo.seeu.bayes.WordClassCounter;
import ca.uwo.seeu.hadoop.features.*;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class SEEUJobDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("WordClassCounter", WordClassCounter.class, 
                "A map/reduce program that count class-word occurrence from database.");
      pgd.addClass("HTMLExtractor", HadoopHTMLTextFeatureExtractor.class, 
                   "A map/reduce program that extract txt features from HTML file and sotre them into databse.");      
      pgd.addClass("HTMLAnchorTextExtractor", HadoopHTMLAnchorTextFeatureExtractor.class, 
              "A map/reduce program that extract anchor txt features from HTML file and sotre them into databse.");
      pgd.driver(argv);
      
      // Success
      exitCode = 0;
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
