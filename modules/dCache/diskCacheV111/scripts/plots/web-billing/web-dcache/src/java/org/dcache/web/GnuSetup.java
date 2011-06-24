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
package org.dcache.web;

import java.sql.Date;
import java.util.ArrayList;
/**
 * @author podstvkv
 *
 */
public class GnuSetup {

    private ArrayList<String> commands;
    private ArrayList<String> datasources;
    private ArrayList<String> styles;
    private ArrayList<String> titles;
    private String output;

    public GnuSetup() {
        commands = new ArrayList<String>() ;
        datasources  = new ArrayList<String>();
        styles  = new ArrayList<String>();
        titles  = new ArrayList<String>();
        commands.add("set size 1.5,1.0");
        commands.add("set terminal postscript eps color solid 'Arial' 17");
        commands.add("set grid");
        commands.add("set timestamp");
        commands.add("set boxwidth 3000 absolute");
        commands.add("set style fill solid 0.5");
        //
        commands.add("set title 'Plot'");
        commands.add("set ylabel 'Y'");
        commands.add("set xlabel 'X'");
        //
//        commands.add("set timefmt '%H:%M'");
//        commands.add("set xdata time");
//        commands.add("set xrange ['00:00':'23:59']");
//        commands.add("set format x '%H:%M'");
    }

    public GnuSetup(String[] adds) {
        this();
        for (int i = 0; i < adds.length; i++) {
            commands.add(adds[i]);
        }
    }

    public GnuSetup setTitle(String title) {
        commands.add("set title '"+title+"'");
        return this;
    }

    public GnuSetup setYlabel(String label) {
        commands.add("set ylabel '"+label+"'");
        return this;
    }

    public GnuSetup setXlabel(String label) {
        commands.add("set xlabel '"+label+"'");
        return this;
    }

    public GnuSetup setXrange(String start, String stop) {
        commands.add("set xrange ['"+start+"':'"+stop+"']");
        return this;
    }

    public GnuSetup setXrange(Date start, Date stop) {
        commands.add("set xrange ['"+start+"':'"+stop+"']");
        return this;
    }

    public GnuSetup setYrange(String start, String stop) {
        commands.add("set yrange ['"+start+"':'"+stop+"']");
        return this;
    }

    public GnuSetup setYrange(Date start, Date stop) {
        commands.add("set yrange ['"+start+"':'"+stop+"']");
        return this;
    }

/*
    public void setXdata(String fmt) {
        commands.add("set xdata "+fmt);
        if (fmt.startsWith("time")) {
            commands.add("set xrange ['00:00':'23:59']");
            commands.add("set format x '%H:%M'");
        } else {
            commands.add("set xrange [ : ]");
            commands.add("set format x '%g'");
        }
    }
 */

    public GnuSetup setYlog(String fmt) {
        return setYlog(fmt, 1.0);
    }

    public GnuSetup setYlog(String fmt, double min) {
        if (fmt.startsWith("log")) {
            commands.add("set log y");
            commands.add("set yrange ["+min+":*]");
        }
        return this;
    }

    public GnuSetup setOutput(String name) {
        output = "set output '"+name+".eps"+"'";
        return this;
    }

    public GnuSetup add(String value) {
        commands.add(value);
        return this;
    }

    public void addDataSrcName(String src) {
        boolean add = datasources.add(src);
    }

    public String getDataSrcName(int n) {
        return datasources.get(n);
    }

    public void addDataStyle(String style) {
        boolean add = styles.add(style);
    }

    public String getDataStyle(int n) {
        return styles.get(n);
    }

    public void addDataSetTitle(String title) {
        boolean add = titles.add(title);
    }

    public String getDataSetTitle(int n) {
        return titles.get(n);
    }

    public int getStylesSize() {
        return styles.size();
    }

    public int size() {
        return commands.size()+1;
    }

    /**
     * Returns i-th command from the commands
     * @param i
     * @return
     */
    public String get(int i) {
        if (i < commands.size()) {
            return (String)commands.get(i);
        }
        return output;
    }


    public String toString() {
        return commands.toString();
    }
}
