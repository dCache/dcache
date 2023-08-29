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
package org.dcache.qos.services.verifier.data;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.dcache.qos.data.QoSMessageType;
import org.dcache.util.BoundedCachedExecutor;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.SignalAware;
import org.springframework.beans.factory.annotation.Required;

/**
 *  Handles the configuration and lifecycle of the queues.
 */
public class VerifyOperationQueueIndex {

    public static class QueueType {
        String name;
        String description;
        List<QoSMessageType> messageTypes;
        ExecutorService executorService;
        int index;

        public int getIndex() {
            return index;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public void setMessageTypes(List<QoSMessageType> messageTypes) {
            this.messageTypes = messageTypes;
        }

        public void setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
        }

        public boolean equals(Object other) {
            if (!(other instanceof QueueType)) {
                return false;
            }

            return ((QueueType)other).index == index;
        }

        public int hashCode() {
            return Objects.hashCode(index);
        }

        public String toString() {
            return "QueueType[" + index + ": " + name + "](" + description + ")";
        }
    }

    private List<QueueType> queueTypes;
    private Map<QoSMessageType, QueueType> typeMap;
    private VerifyOperationQueue[] operationQueues;
    private ExecutorService queueExecutor;

    public void configure(VerifyOperationManager manager) {
        Preconditions.checkNotNull("No queue types configured.", queueTypes);

        int numberOfQueues = queueTypes.size();
        operationQueues = new VerifyOperationQueue[queueTypes.size()];
        typeMap = new HashMap<>();

        for (int index = 0; index < numberOfQueues; ++index) {
            QueueType type = queueTypes.get(index);
            type.index = index;
            type.messageTypes.forEach(mt -> typeMap.put(mt, type));
            operationQueues[index] = new VerifyOperationQueue(type, manager);
        }

        /*
         *  Configure the executor with exactly the number of threads per queues.
         */
        queueExecutor = new BoundedCachedExecutor(numberOfQueues);
    }

    public VerifyOperationQueue getQueue(QoSMessageType type) {
        return operationQueues[typeMap.get(type).index];
    }

    @Required
    public void setQueueTypes(List<QueueType> queueTypes) {
        this.queueTypes = queueTypes;
    }

    public void signalAll() {
        Arrays.stream(operationQueues).forEach(SignalAware::signal);
    }

    public void startQueues() {
        Arrays.stream(operationQueues).map(FireAndForgetTask::new).forEach(queueExecutor::submit);
    }

    public void stopQueues() {
        queueExecutor.shutdownNow();
    }
}
