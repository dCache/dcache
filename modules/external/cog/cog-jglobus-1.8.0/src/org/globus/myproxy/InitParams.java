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
package org.globus.myproxy;

/**
 * Holds the parameters for the <code>put</code> operation.
 */
public class InitParams
    extends Params {
    
    private String retriever;
    private String renewer;
    private String credentialName;
    private String credentialDescription;

    public InitParams() {
        super(MyProxy.PUT_PROXY);
    }

    public void setCredentialName(String credentialName) {
        this.credentialName = credentialName;
    }

    public String getCredentialName() {
        return this.credentialName;
    }

    public void setCredentialDescription(String description) {
        this.credentialDescription = description;
    }

    public String getCredentialDescription() {
        return this.credentialDescription;
    }

    public void setRetriever(String retriever) {
        this.retriever = retriever;
    }

    public String getRetriever() {
        return this.retriever;
    }

    public void setRenewer(String renewer) {
        this.renewer = renewer;
    }

    public String getRenewer() {
        return this.renewer;
    }

    /**
     * If the passpharse is not set returns
     * an empty string.
     */
    public String getPassphrase() {
        String pwd = super.getPassphrase();
        return (pwd == null) ? "" : pwd;
    }

    protected String makeRequest(boolean includePassword) {
        StringBuffer buf = new StringBuffer();
        buf.append(super.makeRequest(includePassword));
        
        add(buf, RETRIEVER, retriever);
        add(buf, RENEWER, renewer);
        add(buf, CRED_NAME, credentialName);
        add(buf, CRED_DESC, credentialDescription);
        
        return buf.toString();
    }
}
