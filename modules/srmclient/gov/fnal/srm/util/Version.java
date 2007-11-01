// $Id: Version.java,v 1.12.2.1 2007-04-17 17:02:54 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.14  2006/10/04 21:18:39  timur
// advance version
//
// Revision 1.13  2006/08/02 16:09:28  timur
// next version is 1.24
//
// Revision 1.12  2006/04/03 17:25:23  timur
// do not read/write web service path from the config.xml
//
// Revision 1.11  2006/03/10 17:18:10  neha
// changes by Neha- to display correct version
//
// Revision 1.10  2006/01/26 05:42:13  timur
// updated version
//
// Revision 1.9  2005/11/14 21:13:34  litvinse
// merged from head
//
// Revision 1.8  2005/11/14 17:07:48  litvinse
// bumped up Version to 1.20
//
// Revision 1.7  2005/09/09 14:33:51  timur
// gov/fnal/srm/util/Version.java
//
// Revision 1.6  2005/08/31 07:26:20  timur
// fix spelling
//
// Revision 1.5  2005/07/26 19:46:09  litvinse
// bumped up version number to 1.18 (srm script handles several protocols
// outside of java code (yet))
//
// Revision 1.4  2005/06/15 15:27:55  timur
// changed the version number in Version.java
//
// Revision 1.3  2005/05/25 22:20:50  timur
// added versioning
//
// Revision 1.1  2005/05/25 21:47:48  timur
// added the first version of Version to repository
//
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
 * Version.java
 *
 * Created on May 25, 2005, 4:28 PM
 */

package gov.fnal.srm.util;

/**
 *
 * @author  timur
 */
public class Version {
    private static final String revision = "$Revision: 1.12.2.1 $";
    private static final String tagName ="$Name: not supported by cvs2svn $";
    private static final String date ="$Date: 2006/03/10 11:12:55 CST 2006";
    private static final String version= "1.23.1";
    /** Creates a new instance of Version */
    public Version() {
    }
    
    public String toString() {
        
        String s =  "Storage Resource Manager (SRM) CP Client version "+version+'\n';
        s += "Copyright (c) 2002-2006 Fermi National Accelerator Laboratory\n";
        if(!tagName.equals("$Name: not supported by cvs2svn $")) {
            s +=" cvs tagname: "+tagName+" cvs date: "+tagName;
        }
        return s;
    }
}
