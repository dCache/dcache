/*
 * PinRequest.java
 *
 * Created on December 17, 2007, 5:15 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.services.pinmanager1;

import java.util.Date;
import org.dcache.auth.AuthorizationRecord;
/**
 *
 * @author timur
 */
public class PinRequest {
    private long id;
    private long srmRequestId;
    private long pinId;
    private long creationTime;
    private long expirationTime;
    private transient Pin pin;
    private AuthorizationRecord authorizationRecord;

    /** Creates a new instance of PinRequest */
    public PinRequest(   long id,
        long srmRequestId,
        long pinId,
        long creationTime,
        long expirationTime,
        AuthorizationRecord authorizationRecord) {
        this.id = id;
        this.srmRequestId = srmRequestId;
        this.pinId = pinId;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
        this.authorizationRecord = authorizationRecord;
    }

    public long getId() {
        return id;
    }

    public long getPinId() {
        return pinId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getSrmRequestId() {
        return srmRequestId;
    }

    public Pin getPin() {
        return pin;
    }

    public void setPin(Pin pin) {
        this.pin = pin;
    }

    public String toString() {
        return ""+ id+
            " pinId:"+pinId+
            " srmId:"+srmRequestId+
            " created:"+ new Date(creationTime).toString()+
            " expires:"+ (expirationTime==-1?"Never":new Date(expirationTime).toString())+
            " authRec:"+authorizationRecord;
    }

    public AuthorizationRecord getAuthorizationRecord() {
        return authorizationRecord;
    }

}
