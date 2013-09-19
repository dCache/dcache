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
 * CopyJobFileReader.java
 *
 * Created on November 26, 2003, 12:53 PM
 */
package gov.fnal.srm.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 *
 * @author  timur
 */
public class CopyJobFileReader {
    private String[] sources ;
    private String[] destinations;
    private int size;
    /** Creates a new instance of CopyJobFileReader */
    public CopyJobFileReader(String file) throws IOException {
        FileInputStream fin = new FileInputStream(file);
        Set<String[]> src_dest_set= new HashSet<>();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(fin))) {
            String origline;
            int line_num = 0;
            while ((origline = in.readLine()) != null) {
                line_num++;
                String line = origline.trim();
                if (line.equals("") || line.startsWith("#")) {
                    continue;
                }
                StringTokenizer st = new StringTokenizer(line);
                if (st.countTokens() != 2) {
                    throw new IOException("File format is incorect, line #" +
                            line_num + ": " + origline);
                }
                String source = st.nextToken();
                String dest = st.nextToken();
                src_dest_set.add(new String[]{source, dest});
            }
        } catch (IOException ioe) {
            throw ioe;
        }

        size = src_dest_set.size();
        String[][] src_dest_array = new String[src_dest_set.size()][];
        src_dest_set.toArray(src_dest_array);
        sources = new String[size];
        destinations = new String[size];
        for(int i = 0 ; i < size; ++i) {
            sources[i] = src_dest_array[i][0];
            destinations[i] = src_dest_array[i][1];
        }

    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("# copy job file \n");
        for(int i = 0 ; i < size; ++i) {
            sb.append( sources[i]).append(' ').append( destinations[i]).append('\n');
        }
        return sb.toString();
    }

    /** Getter for property sources.
     * @return Value of property sources.
     *
     */
    public String[] getSources() {
        return this.sources;
    }


    /** Getter for property destinations.
     * @return Value of property destinations.
     *
     */
    public String[] getDestinations() {
        return this.destinations;
    }

    /** Getter for property size.
     * @return Value of property size.
     *
     */
    public int size() {
        return size;
    }

    public static final void main(String[] args) throws IOException {
        if(args == null || args.length != 1) {
            System.err.println(
            "Usage: java gov.fnal.srm.util.CopyJobFileReader <file>");
            System.exit(1);
        }
        System.out.println(new CopyJobFileReader(args[0]).toString());
    }

}
