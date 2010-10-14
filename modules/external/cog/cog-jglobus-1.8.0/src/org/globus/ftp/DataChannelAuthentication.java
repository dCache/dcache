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
   Represents data channel authentication mode.
   Use static variables SELF or NONE.
 **/
public class DataChannelAuthentication {

    public static final DataChannelAuthentication NONE =
	new DataChannelAuthentication("N");

    public static final DataChannelAuthentication SELF = 
	new DataChannelAuthentication("A");

    protected String argument;

    protected DataChannelAuthentication() {
    }

    protected DataChannelAuthentication(String argument) {
	this.argument = argument;
    }

    protected void setArgument(String argument) {
	this.argument = argument;
    }

    public String toFtpCmdArgument() {
	return argument;
    }

    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof DataChannelAuthentication) {
            DataChannelAuthentication otherObj = 
                (DataChannelAuthentication)other;
            return (this.argument.equals(otherObj.argument));
        } else {
            return false;
        }
    }
    
    public int hashCode() {
        return (this.argument == null) ? 1 : this.argument.hashCode();
    }
    
}
