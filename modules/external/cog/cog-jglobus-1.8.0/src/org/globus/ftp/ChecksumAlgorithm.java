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
 * Represents the algorithm used for checksum operation.
 **/
public class ChecksumAlgorithm {

    public static final ChecksumAlgorithm MD5 =
        new ChecksumAlgorithm("MD5");
    
    protected String argument;
    
    public ChecksumAlgorithm(String name) {
        this.argument = name;
    }
    
    public String toFtpCmdArgument() {
        return argument;
    }
    
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof ChecksumAlgorithm) {
            ChecksumAlgorithm otherObj = 
                (ChecksumAlgorithm)other;
            return (this.argument.equals(otherObj.argument));
        } else {
            return false;
        }
    }
    
    public int hashCode() {
        return (this.argument == null) ? 1 : this.argument.hashCode();
    }
    
}
