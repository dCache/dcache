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

import java.util.Vector;
import java.util.Enumeration;

/** Simple wrapper for org.globus.common.MVHashtable used in MDS applications.
 * 
 * @see org.globus.common.MVHashtable
 */
public class MDSResult extends org.globus.common.MVHashtable {
  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();

    Enumeration dnEnum = keys();
    while ( dnEnum.hasMoreElements() ) {
      String dn = (String)dnEnum.nextElement();
      Vector values = get( dn );

      Enumeration valueEnum = values.elements();
      while ( valueEnum.hasMoreElements() ) {
	stringBuffer.append( dn + "=" + valueEnum.nextElement() + "\n" );
      }
    }
    return stringBuffer.toString();
  }
}
