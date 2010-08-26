// $Id: Client.java,v 1.4 2004-07-02 20:14:21 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.3  2004/06/30 21:57:03  timur
//  added retries on each step, added the ability to use srmclient used by srm copy in the server, added srm-get-request-status
//

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

/*
 * @(#)Client.java	0.9 05/27/02
 *
 * Copyright 2002 Fermi National Accelerator Lab. All rights reserved.
 * FNAL PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package gov.fnal.srm.client;

import electric.registry.Registry;
import gov.fnal.srm.Manager;
import gov.fnal.srm.SRMException;

/**
 * A Storage Resourse Manager API Interface
 * which is implemented by the SRM Server
 * Manager.java, Mon May 27 10:32:35 2002
 *
 * @author Timur Perelmutov
 * @author CD/ISD
 * @version 	0.9, 27 May 2002
 */

public class Client {
    
    public static final short DEFAULTPORT=8004;
    public static final String DEFAULTPATH="srm/srmserver.wsdl";
    
    private Manager manager;
    public Client() throws SRMException {
        this("localhost");
    }
    
    public Client(String host) throws SRMException {
        this(host,DEFAULTPORT);
    }
    
    public Client(String host,short port) throws SRMException {
        this(host,port,DEFAULTPATH);
    }
    
    public Client(String host,short port,String path) throws SRMException {
        this("http",host,port,path);
    }
    
    public Client(String protocol, String host,short port,String path) throws SRMException {
        String wsdl_url =protocol+"://" + host + ":"+Short.toString(port)+"/"+path;
        try {
            manager =(Manager) Registry.bind(wsdl_url,Manager.class);
        }
        catch(Exception e) {
            throw new SRMException(e.getMessage());
        }
        if(manager == null) {
            throw new SRMException();
        }
    }
    
    
    public Manager getManagerConnection() {
        return manager;
    }
    
    /**
     * Main </p>
     *
     * @param  argv
     *         contains command line arguments
     * @return return value description
     *
     * @throws
     *
     */
    
    public static void main(String[] argv) {
        Client client=null;
        try {
            if(argv.length <1) {
                client = new Client();
            }
            else {
                client = new Client(argv[0]);
            }
        }
        catch(SRMException srme) {
            srme.printStackTrace();
            return;
        }
        Manager manger = client.getManagerConnection();
        System.out.println("got connection!!!");
    }
}
