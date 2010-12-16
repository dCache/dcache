/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellMessage;
import org.dcache.auth.AuthorizationRecord;
import javax.security.auth.Subject;


/**
 *
 * @author timur
 */
public interface PinManagerJob {

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
     * @return the clientHost
     */
    String getClientHost();

    /**
     * @return the lifetime
     */
    long getLifetime();

    /**
     * @return the pinId
     */
    Long getPinId();

    /**
     * @return the pinManagerMessage
     */
    PinManagerMessage getPinManagerMessage();

    /**
     * @return the pinRequestId
     */
    Long getPinRequestId();

    /**
     * @return the pnfsId
     */
    PnfsId getPnfsId();

    /**
     * @return the pnfsPath
     */
    String getPnfsPath();

    /**
     * @return the srmRequestId
     */
    long getSrmRequestId();

    /**
     * @return the storageIInfo
     */
    StorageInfo getStorageInfo();

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
     * @param pnfsId the pnfsId to set
     */
    void setPnfsId(PnfsId pnfsId);

    /**
     * @param pnfsPath the pnfsPath to set
     */
    void setPnfsPath(String pnfsPath);

    /**
     * @param info the StorageInfo to set
     */
    void setStorageInfo(StorageInfo info);

    /**
     * @param task the SMCTask to set
     */
    void setSMCTask(SMCTask task);

}
