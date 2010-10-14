/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.io.gass.server;

/**
 * This class defines a job output listener.
 */
public interface JobOutputListener {
  
  /**
   * It is called whenever the job's output
   * has been updated. 
   *
   * @param output new output
   */
  public void outputChanged(String output);
  
  /**
   * It is called whenever job finished
   * and no more output will be generated.
   */
  public void outputClosed();

}
