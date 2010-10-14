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
package org.globus.ftp;

/**
   CKSM options as defined in GridFTP.
 */
public class ChecksumOptions extends Options {

    protected ChecksumAlgorithm algo;

    /**
       @param cmd command whose options are represent by this object
     */
    public ChecksumOptions(ChecksumAlgorithm algo) {
        super("CKSM");
	this.algo = algo;
    }

    /**
       Subclasses should implement this method. It should
       return the right side of the options line,
       in the format of OPTS command. It should not include the
       command name.
     */
    public String getArgument() { return algo.toFtpCmdArgument(); }
    
}
