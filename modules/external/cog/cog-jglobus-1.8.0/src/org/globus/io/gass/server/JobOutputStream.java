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

import java.io.OutputStream;
import java.io.IOException;

/**
 * This is a small class that allows to redirect
 * a job's output to a custom job output listener.
 * That is, a listener that presents/displays the
 * job output in a specific way. For example, this
 * class can be used to redirect a job's output
 * to a window.
 * <p>
 * This class is specificaly designed for jobs
 * that generate textual output. Binary data
 * might not be handled correctly.
 */
public class JobOutputStream extends OutputStream {

  protected JobOutputListener listener;
  
  /**
   * Creates a job output stream with a specific
   * job output listener to which the job output
   * will be redirected to.
   *
   * @param jobListener an instance of the job output 
   *        listener. Cannot be null.
   */
  public JobOutputStream(JobOutputListener jobListener) {
    if (jobListener == null) {
      throw new IllegalArgumentException("jobListener cannot be null");
    }
    listener = jobListener;
  }
  
  /**
   * Converts the byte array to a string and forwards
   * it to the job output listener.
   * <BR>Called by the GassServer.
   */
  public void write(byte[] b, int off, int len) 
       throws IOException {
	 String s = new String(b, off, len);
	 listener.outputChanged(s);
  }
  
  /**
   * Converts the int to a string and forwards
   * it to the job output listener.
   * <BR>Called by the GassServer.
   */
  public void write(int b) 
       throws IOException {
	 listener.outputChanged(String.valueOf(b));
  }
  
  /**
   * Notifies the job output listener that
   * no more output will be produced.
   * <BR>Called by the GassServer.
   */
  public void close() 
       throws IOException {
	 listener.outputClosed();
  }
  
}
