/*
 * GridProtocolInfo.java
 *
 * Created on November 6, 2007, 4:55 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package diskCacheV111.vehicles;

import org.dcache.auth.AuthorizationRecord;
/**
 *
 * @author timur
 */
public interface GridProtocolInfo extends ProtocolInfo {

    public AuthorizationRecord getAuthorizationRecord();

}
