/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import org.dcache.auth.AuthorizationRecord;
import javax.security.auth.Subject;


/**
 *
 * @author timur
 */
public interface PinManagerJob<T extends Message> {

    void failResponse(Object reason, int rc);

    /**
     * @return the authRecord
     */
    AuthorizationRecord getAuthorizationRecord();

    /**
     * @return the subject
     */
    Subject getSubject();

    /**
     * @return the lifetime
     */
    long getLifetime();

    /**
     * @return the pinId
     */
    Long getPinId();

    /**
     * @return the message
     */
    T getMessage();

    /**
     * @return the pinRequestId
     */
    Long getPinRequestId();

    /**
     * @return the pnfsId
     */
    PnfsId getPnfsId();

    /**
     * @return the srmRequestId
     */
    long getSrmRequestId();

    /**
     *
     * @return type of job
     */
    PinManagerJobType getType();


    void returnFailedResponse(Object reason);

    void returnResponse();

    /**
     * @param pinId the pinId to set
     */
    void setPinId(Long pinId);

    /**
     * @param pinRequestId the pinRequestId to set
     */
    void setPinRequestId(Long pinRequestId);

    /**
     * @param task the SMCTask to set
     */
    void setSMCTask(SMCTask task);

}
