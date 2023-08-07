/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.qos;

import java.util.ArrayList;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *  Uses the default specification objects to populate the policy from its JSON representation.
 */
public class DefaultQoSPolicyJsonDeserializer {

    private static final String NAME = "name";
    private static final String STATES = "states";
    private static final String DURATION = "duration";
    private static final String MEDIA = "media";
    private static final String STORAGE_ELEMENT = "storageElement";
    private static final String TYPE = "type";
    private static final String NUMBER_OF_COPIES = "numberOfCopies";
    private static final String PARTITION_KEYS = "partitionKeys";
    private static final String INSTANCE = "instance";

    public static QoSPolicy fromJsonString(String json) {
        return deserializePolicy(new JSONObject(json));
    }

    private static QoSPolicy deserializePolicy(JSONObject object) {
        QoSPolicy policy = new QoSPolicy();
        policy.setName(object.getString(NAME));
        JSONArray stateArray = object.getJSONArray(STATES);
        int len = stateArray.length();
        List<QoSState> states = new ArrayList<>();
        for (int i = 0; i < len; ++i) {
            states.add(deserializeState(stateArray.getJSONObject(i)));
        }
        policy.setStates(states);
        return policy;
    }

    private static QoSState deserializeState(JSONObject jsonState) {
        QoSState state = new QoSState();
        if (jsonState.has(DURATION)) {
            state.setDuration(jsonState.getString(DURATION));
        }
        List<QoSStorageMediumSpecification> media = new ArrayList<>();
        JSONArray mediaArray = jsonState.getJSONArray(MEDIA);
        int len = mediaArray.length();
        for (int i = 0; i < len; ++i) {
            media.add(deserializeStorageElement(mediaArray.getJSONObject(i)));
        }
        state.setMedia(media);
        return state;
    }

    private static QoSStorageMediumSpecification deserializeStorageElement(JSONObject jsonMedia) {
        switch(jsonMedia.getEnum(QoSStorageMedium.class, STORAGE_ELEMENT)) {
            case DISK:
                DefaultQoSDiskSpecification diskSpec = new DefaultQoSDiskSpecification();
                if (jsonMedia.has(TYPE)) {
                    diskSpec.setType(jsonMedia.getString(TYPE));
                }
                diskSpec.setNumberOfCopies(jsonMedia.getInt(NUMBER_OF_COPIES));
                if (jsonMedia.has(PARTITION_KEYS)) {
                    List<String> partitionKeys = new ArrayList<>();
                    JSONArray keyArray = jsonMedia.getJSONArray(PARTITION_KEYS);
                    int len = keyArray.length();
                    for (int i = 0; i < len; ++i) {
                        partitionKeys.add(keyArray.getString(i));
                    }
                    diskSpec.setPartitionKeys(partitionKeys);
                }
                return diskSpec;
            case HSM:
            default:
                DefaultQoSTapeSpecification tapeSpec = new DefaultQoSTapeSpecification();
                if (jsonMedia.has(TYPE)) {
                    tapeSpec.setType(jsonMedia.getString(TYPE));
                }
                if (jsonMedia.has(INSTANCE)) {
                    tapeSpec.setInstance(jsonMedia.getString(INSTANCE));
                }
                return tapeSpec;
        }
    }

    private DefaultQoSPolicyJsonDeserializer() {
        // static singleton
    }
}
