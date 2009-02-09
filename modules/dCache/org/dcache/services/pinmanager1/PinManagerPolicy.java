/*
 * PinManagerPolicy.java
 *
 * Created on September 16, 2008, 4:27 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;
import org.dcache.auth.AuthorizationRecord;

/**
 *
 * @author timur
 */
public interface PinManagerPolicy {
    public boolean canUnpin(AuthorizationRecord requester, PinRequest pinRequest);
    public boolean canExtend(AuthorizationRecord requester, PinRequest pinRequest);

}
