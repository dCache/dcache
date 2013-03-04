// $Id: LambdaStationMap.java,v 1.3 2006/06/15 20:47:50 moibenko Exp $
// $Log: LambdaStationMap.java,v $
// Revision 1.3  2006/06/15 20:47:50  moibenko
// fixed ambiguity in url processing
//
// Revision 1.1  2006/03/23 22:36:46  moibenko
// maps domains to LS sites.
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


package org.dcache.srm.qos.lambdastation;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

//import org.w3c.dom.Element;
//import org.w3c.dom.NodeList;
//import org.w3c.dom.Comment;
// for writing xml
//import javax.xml.transform.Transformer;
//import javax.xml.transform.TransformerFactory;
//import javax.xml.transform.TransformerException;
//import javax.xml.transform.TransformerConfigurationException;
//import javax.xml.transform.dom.DOMSource;
//import javax.xml.transform.stream.StreamResult;
//import java.io.File;
//import java.io.FileWriter;


public class LambdaStationMap {
    private List<Site> Sites = new LinkedList<>();


    private class Site {
        public String domain;
        public String name;
        public boolean enabled;
        public Site(String domain, String name, boolean enabled) {
            this.domain = domain;
            this.name = name;
            this.enabled = enabled;
        }
    }

    /** Creates a new instance of Configuration */
    public LambdaStationMap() {

    }

    public LambdaStationMap(String map_file)
    {
        try
        {
            read(map_file);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }


    public void read(String file) throws Exception {
        String domain = "";
        String name = "";
        DocumentBuilderFactory factory =
        DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(file);
        Node root =document.getFirstChild();
        for(;root != null && !"l-station-map".equals(root.getNodeName());
        root = document.getNextSibling()) {
        }
        if(root == null) {
            System.err.println(" error, root element \"l-station-map\" is not found");
            throw new IOException();
        }


        if(root != null && root.getNodeName().equals("l-station-map")) {


           Node node = root.getFirstChild();
           for(;node != null; node = node.getNextSibling()) {
                if(node.getNodeType()== Node.ELEMENT_NODE) {
		    //System.out.println("ELEMENT "+node.getNodeName());

                    if (node.getNodeName().equals("site")){
                        boolean enabled = true;
                        Node child = node.getFirstChild();
			//System.out.println("CHILD1 ELEMENT "+child.getNodeName());
                        for(;child != null; child = child.getNextSibling()) {
                            if(child.getNodeType() == Node.ELEMENT_NODE) {
				//System.out.println("CHILD ELEMENT "+child.getNodeName());
                               Node g_c = child.getFirstChild();
                               if (g_c.getNodeType() == Node.TEXT_NODE) {
                                    Text t  = (Text)g_c;
                                    String text_value = t.getData().trim();
                                    String node_name = child.getNodeName().trim();
				    //System.out.println("NODE="+node_name+" VAL="+text_value);
                                   switch (node_name) {
                                   case "domain":
                                       domain = text_value;
                                       break;
                                   case "enable-LS":
                                       String inh = text_value;
                                       if (inh.startsWith("n")) {
                                           enabled = false;
                                       }
                                       break;
                                   case "name":
                                       name = text_value;
                                       break;
                                   }
                               }
                            }

			    else if (child.getNodeType() == Node.TEXT_NODE){
				//skip garbage
				Text t1  = (Text)child;
				String tv = t1.getData().trim();
				//System.out.println("TEXT "+tv);
				continue;


			    }
                            if(child.getNodeType() == Node.TEXT_NODE){
                                Text t  = (Text)child;
                                String text_value = t.getData().trim();
                                break;
                            }
                        }
                        set(domain, name, enabled);

                     }
                }
            }
        }

    }


    private void set(String name, String value, boolean enabled) {
        name=name.trim();
        value=value==null?null:value.trim();
        Site site = new Site(name, value, enabled);
        this.Sites.add(site);
    }

    public void say(String words) {
        System.out.println("LS_MAP: "+words);
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Lambda station Map:");
        for (Object Site : Sites) {
            Site s = (Site) Site;
            sb.append("\n\tdomain=").append(s.domain);
            sb.append("\tname=").append(s.name);
            if (s.enabled) {
                sb.append("\tLS enabled=").append("yes");
            } else {
                sb.append("\tLS enabled=").append("no");
            }
        }
        return sb.toString();
    }

    public String getName(String url) {
	StringTokenizer urlTokenizer
	    = new StringTokenizer(url, ":");
        String tok = (String) urlTokenizer.nextElement();
	tok = (String) urlTokenizer.nextElement(); // first is srm, file, etc
        //say("MAP="+toString()+"token="+tok);
        for (Object Site : Sites) {
            //say("GETNAME");
            Site s = (Site) Site;
            if (tok.contains(s.domain)) {
                //say("URL="+url+" domain="+s.domain+" name="+s.name);
                return s.name;
            }
        }
        return null;
    }

    public boolean enabled(String url) {
        for (Object Site : Sites) {
            Site s = (Site) Site;
            if (url.contains(s.domain)) {
                return s.enabled;
            }
        }
        return false;
    }

    /*
    public static void main(String argv[])
    {
        if (argv.length != 1) {
            System.err.println("Usage: java LambdaStationMap filename");
            System.exit(1);
        }
        System.out.println("file "+argv[0]);
        LambdaStationMap lsMap = new LambdaStationMap(argv[0]);
        String str = lsMap.toString();
        System.out.println("MAP:"+str);
    }// main
    */




}
