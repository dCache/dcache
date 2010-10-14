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
package org.globus.ftp;

import org.globus.ftp.exception.ClientException;

/**
 * Represents parameters of an FTP session between a client and a server. For
 * instance, a third party transfer will be represented by two sessions: one
 * between the client and the server A, and the other between the client and the
 * server B. <br>
 * Public static variables are interpreted as follows:
 * <ul>
 * <li> prefix TYPE denotes transfer type
 * <li> prefix MODE denotes transfer mode
 * <li> prefix SERVER denotes server mode
 * </ul>
 */
public class Session {
    public static final int TYPE_IMAGE = 1;

    public static final int TYPE_ASCII = 2;

    public static final int TYPE_LOCAL = 3;

    public static final int TYPE_EBCDIC = 4;

    public static final int MODE_STREAM = 1;

    public static final int MODE_BLOCK = 2;

    public static final int SERVER_PASSIVE = 1;

    public static final int SERVER_ACTIVE = 2;

    // may apply anywhere where
    // the variables above do apply
    public static final int SERVER_DEFAULT = -1;

    // variables that hold server state;
    // used mostly in complex functions like 3rd party transfer

    // equal to MODE_xxx
    public int transferMode = MODE_STREAM;

    // equal to TYPE_xxx
    public int transferType = TYPE_ASCII;

    /**
     * Can be SERVER_PASSIVE, SERVER_ACTIVE, or SERVER_DEFAULT. The latter means
     * that the mode has not been set explicitly, so the server should act as
     * default: passive on the standard port L-1.
     */
    public int serverMode = SERVER_DEFAULT;

    public int protectionBufferSize = SERVER_DEFAULT;

    public boolean authorized = false;

    /* caches FEAT command reply */
    public FeatureList featureList = null;

    public HostPort serverAddress = null;

    public static final int DEFAULT_MAX_WAIT = 1000 * 30; // 30 secs

    public static final int DEFAULT_WAIT_DELAY = 2000;

    /**
     * This variable directly affects only the client. After requesting data
     * transfer, client will wait on the control channel maxWait miliseconds,
     * polling for replies every waitDelay seconds. If reply does not arrive
     * after maxWait, client will abort.
     */
    public int maxWait = DEFAULT_MAX_WAIT;

    /**
     * This variable directly affects only the client. After requesting data
     * transfer, client will wait on the control channel maxWait miliseconds,
     * polling for replies every waitDelay seconds. If reply does not arrive
     * after maxWait, client will abort.
     */
    public int waitDelay = DEFAULT_WAIT_DELAY;

    /**
     * Ensures that settings of 2 servers match each other so that the servers
     * are capable of performing a transfer between themselves. The parameters
     * of both sessions must either both be set correctly, or both undefined.
     * <br>
     * Detailed rules: Two sessions match if their transfer type, mode, and
     * protection buffer sizes match. Additionally, if one party is passive, the
     * other must be active. If any of the variables are set to SERVER_DEFAULT,
     * sessions are considered matching.
     * 
     * @throws ClientException
     *             if sessions do not match
     */
    public void matches(Session other) throws ClientException {

        compareTransferParams(other);
        compareServerMode(other);
    }

    /**
     * defines how to compare parameters: authorized, PBSZ, MODE, TYPE
     */
    protected void compareTransferParams(Session other) throws ClientException {
        if (!this.authorized || !other.authorized)
            throw new ClientException(ClientException.NOT_AUTHORIZED,
                    "Need to perform authorization first");

        // synchronize protection buffer size
        if (this.protectionBufferSize != other.protectionBufferSize) {
            throw new ClientException(ClientException.PBSZ_DIFFER);
        }
        // synchronize type
        if (this.transferType != other.transferType) {
            throw new ClientException(ClientException.TRANSFER_TYPE_DIFFER);
        }
        // synchronize type
        if (this.transferMode != other.transferMode) {
            throw new ClientException(ClientException.TRANSFER_MODE_DIFFER);
        }
    }

    /**
     * checks that active / passive sides are correctly set
     */
    protected void compareServerMode(Session other) throws ClientException {

        if (serverMode == SERVER_DEFAULT && 
            other.serverMode == SERVER_DEFAULT) {
            // this is OK
        } else {
            // active and passive side had already been set;
            // make sure that it has been done correctly:
            // either server can be active
            // providing that the other is passive

            // if this server mode has been defined,
            // but the other has not, we can't proceed
            if (this.serverMode == SERVER_DEFAULT ||
                other.serverMode == SERVER_DEFAULT) {
                throw new ClientException(ClientException.BAD_SERVER_MODE,
                        "Only one server has been defined as active or passive");
            }

            // both servers cannot have the same mode
            if (other.serverMode == this.serverMode) {
                String modeStr = (this.serverMode == SERVER_PASSIVE) ? "passive"
                        : "active";
                throw new ClientException(ClientException.BAD_SERVER_MODE,
                        "Both servers are " + modeStr);
            }
        }
    }// compareServerMode

}
