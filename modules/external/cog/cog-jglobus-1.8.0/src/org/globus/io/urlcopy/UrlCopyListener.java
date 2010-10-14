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
package org.globus.io.urlcopy;

public interface UrlCopyListener {
  
    /**
     * This function is contniuosly called during url transfers.
     *
     * @param transferedBytes number of bytes currently trasfered
     *                        if -1, then performing thrid party transfer
     * @param totalBytes      number of total bytes to transfer
     *                        if -1, the total size in unknown.
     */
    public void transfer(long transferedBytes, long totalBytes);
    
    /**
     * This function is called only when an error occurs.
     *
     * @param exception  the actual error exception
     */
    public void transferError(Exception exception);

    /**
     * This function is called once the transfer is completed
     * either successfully or because of a failure. If an error occurred
     * during the transfer the transferError() function is called first.
     */ 
    public void transferCompleted();
    
}
