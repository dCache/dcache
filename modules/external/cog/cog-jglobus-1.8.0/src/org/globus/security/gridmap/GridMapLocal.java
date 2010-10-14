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
package org.globus.security.gridmap;

/**
 * Gridmap implementation with a local username lookup feature.
 * If the gridmap is uninitialized the <tt>getUserID</tt>, <tt>getUserIDs</tt>,
 * and <tt>checkUser</tt> functions will effectively ignore their 
 * <tt>globusID</tt> parameter. For example, <tt>getUserID</tt> and
 * <tt>getUserIDs</tt> will return the local user name for any
 * <tt>globusID</tt>.
 */
public class GridMapLocal extends GridMap {
    
    public String[] getUserIDs(String globusID) {
        String [] userIDs = super.getUserIDs(globusID);
        
        if (userIDs == null && this.map == null) {
            String user = getLocalUsername();
            return (user == null) ? null : new String[] {user};
        }
        
        return userIDs;
    }
    
    public boolean checkUser(String globusID, String userID) {
        boolean result = super.checkUser(globusID, userID);
        
        if (!result && this.map == null) {
            String user = getLocalUsername();
            return (user == null) ? false : user.equalsIgnoreCase(userID);
        }
        
        return result;
    }
    
    private String getLocalUsername() {
        String user = System.getProperty("user.name");
        if (user == null) {
            return null;
        }
        String tmpUser = user.toLowerCase();
        return (tmpUser.equals("root") || tmpUser.equals("administrator")) 
            ? null : user;
    }
    
}
