/*
 * SimplePinManagerPolicyImpl.java
 *
 * Created on September 16, 2008, 4:30 PM
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
public class SimplePinManagerPolicyImpl implements PinManagerPolicy {

    /** Creates a new instance of SimplePinManagerPolicyImpl */
    public SimplePinManagerPolicyImpl() {
    }


    public boolean canUnpin(AuthorizationRecord requester, PinRequest pinRequest){
        AuthorizationRecord creator = pinRequest.getAuthorizationRecord();
        if(requester == null ) {
            if(creator == null) {
                return true;
            } else {
                return false;
            }
        }
        /* only admin can unpin anonymous pin requests */
        if(creator == null) {
            return false;
        }

         if(creator.getId() == requester.getId()) {
            return true;
        }

        if( creator.getUid() == requester.getUid() ) {
            return true;
        }

        if(creator.getVoGroup() != null ) {
            if(creator.getVoGroup().equals(requester.getVoGroup())) {
                if(creator.getVoRole() != null) {
                    return creator.getVoRole().equals(requester.getVoRole());

                } else {
                    return requester.getVoRole() == null;
                }
            } else {
                return false;
            }
        }

        return false;
    }

    public boolean canExtend(AuthorizationRecord requester, PinRequest pinRequest) {
        return canUnpin(requester,pinRequest);
    }

}
