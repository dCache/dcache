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
package org.globus.ftp.exception;

import org.globus.ftp.vanilla.Reply; 

/**
   Indicates that the received reply had different code than
   it had been expected.
 */
public class UnexpectedReplyCodeException extends FTPException {

    private Reply reply;

    public UnexpectedReplyCodeException(int code, String msg, Reply r) {
	super(code,msg);
	this.reply = r;
    }

    public UnexpectedReplyCodeException(Reply r) {
	super(FTPException.UNSPECIFIED, 
	      "Unexpected reply: " + r);
	this.reply = r;
    }

    public Reply getReply() {
	return reply;
    }

 }
