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
package org.dcache.services.bulk.job;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 *  Metadata for optional argument to a bulk job.
 */
public class BulkJobArgumentDescriptor
{
    private final String name;
    private final String description;
    private final boolean required;
    private final String defaultValue;
    private final String valueSpec;

    public BulkJobArgumentDescriptor(String name,
                                     String description,
                                     String valueSpec,
                                     boolean required,
                                     String defaultValue)
    {
        this.name = Preconditions.checkNotNull(name,"name cannot "
                        + "be null.");
        this.description = Preconditions.checkNotNull(name,"description "
                        + "cannot be null.");
        this.valueSpec = Preconditions.checkNotNull(valueSpec,
                                                    "possible values "
                                                                    + "must be "
                                                                    + "specified");
        this.required = required;
        if (!required) {
            Preconditions.checkNotNull(defaultValue, "default value "
                                       + "must be provided if arg is not required.");
            this.defaultValue = defaultValue;
        } else {
            this.defaultValue = null;
        }
    }

    public String getName()
    {
        return name;
    }

    public String getDescription()
    {
        return description;
    }

    public boolean isRequired()
    {
        return required;
    }

    public String getDefaultValue()
    {
        return defaultValue;
    }

    public String getValueSpec()
    {
        return valueSpec;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(name)
               .append(", [")
               .append(valueSpec)
               .append("](required ")
               .append(required)
               .append(") ");
        if (!required) {
            builder.append("(default ").append(defaultValue).append(") ");
        }

        return builder.append("[").append(description).append("]").toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object other)
    {
       if (other == null) {
           return false;
       }

       if (!(other instanceof BulkJobArgumentDescriptor)) {
           return false;
       }

       return name.equals(((BulkJobArgumentDescriptor) other).name);
    }
}
