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
package org.globus.mds;   

import org.globus.common.ChainedException;

/** An Exception class for MDS interactions.

  <p><b> USING THE MDS EXEPTION CLASS </b>
  <blockquote>
  
  The intention of the MDS Exeption class is to provide a simple
  exeption mechanism for MDS related methods. This includes:

  <ol>
  <li> failure to conect to the MDS due to timeout or other problems,
  <li> failure during MDS queries,
  <li> ... .
  </ol>

  In case the Exeption is thrown by another underlaying library, it is
  the task of the user to properly deal with the exceptions. Such
  methods and exceptions can be found in JNDI and Java Networking classes.

  </blockquote>

  <p><b> Important Network related Exceptions</b>
  <blockquote>

  ...
  
  </blockquote>




  <p><b> Important JNDI Exceptions </b>
  <blockquote>
  
  ...

  </blockquote>

  <p><b> Example</b>
  <blockquote>

  See ... MDS.java

  </blockquote>
  **/
 
public class MDSException extends ChainedException {

    private String ldapMessage;
    
    public MDSException(String mdsMessage, String ldapMessage) {
	super(mdsMessage);
	this.ldapMessage = ldapMessage;
    }

    public MDSException(String detail, Throwable exception) {
	super(detail, exception);
    }

    public String getLdapMessage() {
	return ldapMessage;
    }

}
