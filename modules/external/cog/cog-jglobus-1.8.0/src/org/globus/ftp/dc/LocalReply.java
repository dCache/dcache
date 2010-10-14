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
package org.globus.ftp.dc;

import org.globus.ftp.vanilla.Reply;

/**
   Local server communicate with client with a  simplified control channel.
   This is a local, minimal version of Reply, free of overhead
   caused by parsing during construction.
 **/
public class LocalReply extends Reply {

    private static final String MESSAGE = "this LocalReply does not have a message";
    
    public LocalReply(int code) {
	this.message = MESSAGE;
	this.isMultiline = false;
	this.code = code;
	this.category = code / 100;
    }

    public LocalReply(int code, String message) {
	this.message = message;
	this.isMultiline = false;
	this.code = code;
	this.category = code / 100;
    }

}

