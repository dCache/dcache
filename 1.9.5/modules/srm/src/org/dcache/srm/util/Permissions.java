/*
 * Permissions.java
 *
 * Created on January 23, 2003, 2:38 PM
 */

package org.dcache.srm.util;

/**
 *
 * @author  timur
 */

public final class Permissions {
    
     
   
    public static boolean userCanRead(int permissions)
    {
        return ((permissions >> 8) & 1) == 1; 
    }
    
    public static boolean userCanWrite(int permissions)
    {
        return ((permissions >> 7) & 1) == 1; 
    }

    public static boolean userCanExecute(int permissions)
    {
        return ((permissions >> 6) & 1) == 1; 
    }
    
    public static boolean  groupCanRead(int permissions)
    {
        return ((permissions >> 5) & 1) == 1; 
    }
    
    public static boolean groupCanWrite(int permissions)
    {
        return ((permissions >> 4) & 1) == 1; 
    }

    public static boolean groupCanExecute(int permissions)
    {
        return ((permissions >> 3) & 1) == 1; 
    }
    
    public static boolean worldCanRead(int permissions)
    {
        return ((permissions >> 2) & 1) == 1; 
    }
    
    public static boolean worldCanWrite(int permissions)
    {
        return ((permissions >> 1) & 1) == 1; 
    }

    public static boolean worldCanExecute(int permissions)
    {
        return (permissions & 1) == 1; 
    }
    
}
