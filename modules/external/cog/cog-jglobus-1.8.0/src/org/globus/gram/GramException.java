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
package org.globus.gram;

import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Locale;

import org.globus.common.ChainedException;
import org.globus.gram.internal.GRAMProtocolErrorConstants;

/** 
 * This class defeines the Exceptions which are thrown by the various
 * Gram and GramJob methods.
 * This class contains many definitions for error codes of the
 * form ERROR_* in addition to SUCCESS. The error codes are a superset
 * of those defined for the GRAM C client. Additional error codes
 * are added for errors that can occur in the Java code.
 * 
 */
public class GramException extends ChainedException implements GRAMProtocolErrorConstants {

    private static ResourceBundle resources;
    
    static {
	try {
	    resources = ResourceBundle.getBundle("org.globus.gram.internal.errors", 
						 Locale.getDefault());
	} catch (MissingResourceException mre) {
	    System.err.println("org.globus.gram.internal.gramerrors.properties not found");
	}
    }

    public static final int SUCCESS = 0;
    public static final int INIT_CALLBACK_HANDLER_FAILED = 1000;
    public static final int ERROR_JOB_CONTACT_NOT_SET    = 1003;
    
    /**    */
    public static final int CUSTOM_ERROR = 9999;
    
    /**    */
    protected int errorCode;
    
  /**    */
  public GramException() {
    super( getMessage( SUCCESS ) );
    errorCode = SUCCESS;
  }

  /**    */
  public GramException(int ec) {
    super( getMessage( ec ) );
    errorCode = ec;
  }

  public GramException(String msg) {
    super( msg );
    errorCode = CUSTOM_ERROR;
  }

  public GramException(int ec, Throwable ex) {
    super( getMessage( ec ), ex );
    errorCode = ec;
  }

  /**    */
  public void setErrorCode(int ec) {
    errorCode = ec;
  }

  /**    */
  public int getErrorCode() {
    return errorCode;
  }

  public static String getMessage(int errorCode) {
    if (errorCode == SUCCESS) {
      return "Success";
    } else {
      if (resources == null) return "Error code: " + errorCode;

      try {
	return resources.getString(String.valueOf(errorCode));
      } catch (MissingResourceException mre) {
	return "Error code: " + errorCode;
      }
    }
  }

}
