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

package org.dcache.srm.unixfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

// WARINING THIS CLASS IS NOT THREAD SAFE
// Format of : authentication file:
//
// username: uid gid home root <fsroot>
//  principal principal ... 	# principal lines must begin with white space
//  principal ...
// username: ...
//

public class KAuthFile {
    private static final String MAPPING_MARKER="mapping ";
    private static final String AUTH_RECORD_MARKER="login ";
    private static final String PWD_RECORD_MARKER="passwd ";
    private static final String FILE_VERSION_MARKER="version ";
    private static final String VERSION_TO_GENERATE="2.1";

    private static boolean debug;
    private double fileVersion;
    private HashMap<String,UserAuthRecord> auth_records = new HashMap<>();
    private HashMap<String,UserPwdRecord> pwd_records = new HashMap<>();
    private HashMap<String,String> mappings = new HashMap<>();


    private KAuthFile(String filename, boolean convert)
    throws IOException {
        FileReader fr = new FileReader(filename);
        BufferedReader reader = new BufferedReader(fr);
        readFileOld(reader);
    }

    private KAuthFile(InputStream in,boolean convert)
    throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        readFileOld(reader);
    }

    public KAuthFile(String filename)
    throws IOException {
        // read file in
        FileReader fr = new FileReader(filename);
        BufferedReader reader = new BufferedReader(fr);
        read(reader);
        reader.close();
    }
    public KAuthFile(InputStream in)
    throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        read(reader);
    }

    public UserPwdRecord getUserPwdRecord(String username) {
        return pwd_records.get(username);
    }

    private void read(BufferedReader reader)
    throws IOException {
        String line;

        while((line = reader.readLine()) != null) {
            line = line.trim();

            if(line.startsWith(AUTH_RECORD_MARKER)) {
                line = line.substring(AUTH_RECORD_MARKER.length());
                UserAuthRecord rec = readNextUserAuthRecord(line,reader);

                if(rec != null) {

                    auth_records.put(rec.Username,rec);
                }
                else {
                    while( (line = reader.readLine()) != null ) {
                        line=line.trim();
                        if(line.equals("")) {
                            break;
                        }
                    }
                }
            }

            else if( line.startsWith(PWD_RECORD_MARKER)) {

                line = line.substring(PWD_RECORD_MARKER.length());
                UserPwdRecord rec = readNextUserPwdRecord(line);

                if(rec != null) {

                    pwd_records.put(rec.Username,rec);
                }
            }

            else if(line.startsWith(FILE_VERSION_MARKER)) {
                line = line.substring(FILE_VERSION_MARKER.length());
                line = line.trim();
                fileVersion = Double.parseDouble(line);
            }

            else if(line.startsWith(MAPPING_MARKER)) {
                line = line.substring(MAPPING_MARKER.length());
                line = line.trim();
                if(line.charAt(0) != '\"') {
                    continue;
                }
                line=line.substring(1);
                int last_quote = line.lastIndexOf('\"');
                if(last_quote == -1) {
                    continue;
                }
                String principal = line.substring(0,last_quote);
                String default_user_name = line.substring(last_quote+1).trim();
                if(default_user_name != null && default_user_name.length() >0) {
                    mappings.put(principal,default_user_name);
                }
            }
        }
    }

    private UserAuthRecord readNextUserAuthRecord(String line, BufferedReader reader)
    throws IOException {

        line = line.trim();

        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();


        if ( (ntokens < 5 || ntokens > 6) && (fileVersion >= 2.1 && (ntokens < 6 || ntokens > 7) ) ) {
            return null;
        }

        boolean readOnly = false;
        String user = t.nextToken();
        if(fileVersion >= 2.1) {
            String readOnlyToken = t.nextToken();
            if( readOnlyToken.equals("read-only") ) {
                readOnly = true;
            }
        }
        int uid = Integer.parseInt(t.nextToken());
        int gid = Integer.parseInt(t.nextToken());
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = root;

        if( ( ntokens == 6 && fileVersion < 2.1) || (fileVersion >= 2.1 &&  ntokens == 7 ) ) {
            fsroot = t.nextToken();
        }

        HashSet<String> principals = new HashSet<>();

        while( (line =reader.readLine()) != null ) {
            line = line.trim();
            if(line.equals("")) {
                break;
            }
            if( line.startsWith("#") ) {
                continue;
            }
            principals.add(line);
        }

        UserAuthRecord rec =  new UserAuthRecord(user,readOnly,uid,gid,home,root,fsroot,principals);

        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    private UserPwdRecord readNextUserPwdRecord(String line) {
        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( (ntokens < 6 || ntokens > 7) &&
        (fileVersion >= 2.1 && (ntokens < 7 || ntokens > 8) ) ) {
            return null;
        }

        boolean readOnly = false;

        String username = t.nextToken();
        String passwd = t.nextToken();
        if(fileVersion >= 2.1) {
            if( t.nextToken().equals("read-only") ) {
                readOnly = true;
            }
        }
        int uid = Integer.parseInt(t.nextToken());
        int gid = Integer.parseInt(t.nextToken());
        String home = t.nextToken();
        String root = t.nextToken();
        String fsroot = root;
        if( ntokens == 8 ) {
            fsroot = t.nextToken();
        }

        UserPwdRecord rec =  new UserPwdRecord(username,passwd,readOnly,uid,gid,home,root,fsroot);

        if (rec.isValid()) {
            return rec;
        }
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(header);
        sb.append(mapping_section_header);

        sb.append("version " + VERSION_TO_GENERATE + "\n");

        Iterator<String> iter = mappings.keySet().iterator();
        while(iter.hasNext()) {
            String secure_id= iter.next();
            String user = mappings.get(secure_id);
            sb.append(MAPPING_MARKER);
            sb.append('\"').append(secure_id).append("\" ");
            sb.append(user).append('\n');

        }
        sb.append('\n');
        sb.append("# the following are the user auth records\n");
        iter = auth_records.keySet().iterator();
        while(iter.hasNext()) {
            String user= iter.next();
            if(user.indexOf('/') != -1) {
                sb.append("# the following user record should probably be converted to mapping\n");
            }
            UserAuthRecord record = auth_records.get(user);
            sb.append(AUTH_RECORD_MARKER).append(record);
            sb.append('\n');

        }
        sb.append("# the following are the user auth records\n");
        iter = pwd_records.keySet().iterator();
        while(iter.hasNext()) {
            String user= iter.next();
            if(user.indexOf('/') != -1) {
                sb.append("# the following user record should probably be converted to mapping\n");
            }
            UserPwdRecord record = pwd_records.get(user);
            sb.append(PWD_RECORD_MARKER).append(record);
            sb.append('\n');

        }
        return sb.toString();
    }

    public UserAuthRecord getUserRecord(String username) {
        return auth_records.get(username);
    }

    public String getIdMapping(String id) {
        return mappings.get(id);
    }


    public static final void main(String[] args)
    throws IOException {
        KAuthFile file;
        Arguments arguments =new Arguments();
        try {
            arguments = parseArgs(args,arguments);
            String command = arguments.command;
            switch (command) {
            case "dclist":
                if (arguments.help) {
                    System.out.print(dclist_usage);
                    return;
                }

                if (arguments.file != null) {
                    file = new KAuthFile(arguments.file);
                } else {
                    file = new KAuthFile(System.in);
                }
                System.out.print(file.toString());
                break;
            case "convert":
                if (arguments.help) {
                    System.out.print(convert_usage);
                    return;
                }

                if (arguments.file != null) {
                    file = new KAuthFile(arguments.file, true);
                } else {
                    file = new KAuthFile(System.in, true);
                }
                System.out.print(file.toString());
                break;
            case "dcuserlist":
                if (arguments.help) {
                    System.out.print(dcuserlist_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcuserlist(arguments);
                break;
            case "dcuseradd":
                if (arguments.help) {
                    System.out.print(dcuseradd_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcuseradd(arguments);
                file.save(arguments.file);
                break;
            case "dcusermod":
                if (arguments.help) {
                    System.out.print(dcusermod_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcusermod(arguments);
                file.save(arguments.file);
                break;
            case "dcuserdel":
                if (arguments.help) {
                    System.out.print(dcuserdel_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcuserdel(arguments);
                file.save(arguments.file);
                break;
            case "dcmaplist":
                if (arguments.help) {
                    System.out.print(dcmaplist_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcmaplist(arguments);
                break;
            case "dcmappedtolist":
                if (arguments.help) {
                    System.out.print(dcmappedtolist_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcmappedtolist(arguments);
                break;
            case "dcmapadd":
                if (arguments.help) {
                    System.out.print(dcmapadd_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcmapadd(arguments);
                file.save(arguments.file);
                break;
            case "dcmapmod":
                if (arguments.help) {
                    System.out.print(dcmapmod_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcmapmod(arguments);
                file.save(arguments.file);
                break;
            case "dcmapdel":
                if (arguments.help) {
                    System.out.print(dcmapdel_usage);
                    return;
                }

                file = new KAuthFile(arguments.file);
                file.dcmapdel(arguments);
                file.save(arguments.file);
                break;
            default:
                throw new IllegalArgumentException(" command is not recognized:" +
                        command);
            }
        }
        catch(Exception e) {
            System.err.println("error :"+e.getMessage());
            if(debug) {
                e.printStackTrace();
            }
            if(arguments == null || arguments.command == null) {
                System.out.println(usage);
            }
            else if ( arguments.command.equals("dclist") ) {
                System.out.print(dclist_usage);
            } else if ( arguments.command.equals("convert") ) {
                System.out.print(convert_usage);
            } else if ( arguments.command.equals("dcuseradd") ) {
                System.out.print(dcuseradd_usage);
            } else if ( arguments.command.equals("dcusermod") ) {
                System.out.print(dcusermod_usage);
            } else if ( arguments.command.equals("dcuserdel") ) {
                System.out.print(dcuserdel_usage);
            } else if ( arguments.command.equals("dcuserlist") ) {
                System.out.print(dcuserlist_usage);
            } else if ( arguments.command.equals("dcmapadd") ) {
                System.out.print(dcmapadd_usage);
            } else if ( arguments.command.equals("dcmapdel") ) {
                System.out.print(dcmapdel_usage);
            } else if ( arguments.command.equals("dcmapmod") ) {
                System.out.print(dcmapmod_usage);
            } else if ( arguments.command.equals("dcmaplist") ) {
                System.out.print(dcmaplist_usage);
            } else if ( arguments.command.equals("dcmappedtolist") ) {
                System.out.print(dcmappedtolist_usage);
            } else {
                System.out.println(usage);
            }

        }

    }

    public void save(String filename) throws IOException {
        File passwd_file = new File(filename);
        if(passwd_file.exists()) {
            File backup_file = new File(filename+'~');
            passwd_file.renameTo(backup_file);
        }
        FileOutputStream fos = new FileOutputStream(passwd_file);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fos));
        System.out.println("writing to "+passwd_file+" :\n");//+toString());
        try {
            out.write(toString());
            out.flush();
            out.close();
            System.out.println("done writing to "+passwd_file+" :\n");//+toString());
        }
        catch(Exception e) {
            if(debug) {
                e.printStackTrace();
            }
            System.err.println("error saving file "+e);
            File backup_file = new File(filename+'~');
            if(backup_file.exists()) {
                System.out.println("restoring original file");
                passwd_file = new File(filename);
                backup_file.renameTo(passwd_file);
            }
        }
    }

    public void readFileOld(BufferedReader reader)
    throws IOException {

        String line;
        while((line  = reader.readLine()) != null) {
            line = line.trim();
            if( line.startsWith("#") || line.indexOf(":") <= 0 ) {
                line = reader.readLine();
                continue;
            }
            UserAuthRecord rec = readOldAuthRecord(line, reader);
            if ( rec != null && rec.isValid() ) {
                auth_records.put(rec.Username, rec);
                //	System.out.println("Added user <"+rec.Username+">: " + rec);
            }
        }
    }


    private UserAuthRecord readOldAuthRecord(String line, BufferedReader reader)
    throws IOException {
        String Username;		// invalidate
        line = line.trim();
        int colon = line.indexOf(":");
        if( colon <= 0 ) {
            return null;
        }

        String username = line.substring(0,colon);
        line = line.substring(colon+1).trim();

        StringTokenizer t = new StringTokenizer(line);
        int ntokens = t.countTokens();
        if ( ntokens < 4 || ntokens > 5 ) {
            return null;
        }
        int UID = Integer.parseInt(t.nextToken());
        int GID = Integer.parseInt(t.nextToken());
        String Home = t.nextToken();
        String Root = t.nextToken();
        String FsRoot = Root;
        if( ntokens > 4 ) {
            FsRoot = t.nextToken();
        }
        Username = username;		// Now it's valid
        //System.out.println("User: <"+username+">");

        // Read principals
        HashSet<String> Principals = new  HashSet<>();
        line = reader.readLine();
        while( line != null ) {
            if( !line.startsWith(" ") && !line.startsWith("\t") ) {
                break;
            }
            line = line.trim();
            if( line.startsWith("#") ) {	// comment line
                line = reader.readLine();
                continue;
            }
            //System.out.println("Principal line: <"+line+">");
            StringTokenizer lst = new StringTokenizer(line);
            int np = lst.countTokens();
            //System.out.println("Tokens: "+np);
            for( int i = 0; i < np; i++ ) {
                String p = lst.nextToken();
                Principals.add(p);
                //System.out.println("Principal: <"+p+">");
            }
            line = reader.readLine();
        }
        //System.out.println("UserRecord: " + this);
        UserAuthRecord record = new UserAuthRecord(Username,false, UID,GID,Home,Root,FsRoot,Principals);
        return record;
    }

    public void dcuseradd(Arguments arguments) {
        if( arguments.uid == null  ) {
            throw new IllegalArgumentException(" uid is not specified ");
        }
        int uid = arguments.uid;
        if(uid < 1 || uid > 0xFFFF ) {
            throw new IllegalArgumentException(" uid value "+uid+
            " is not in the range [1,65535]");
        }
        if( arguments.gid == null  ) {
            throw new IllegalArgumentException(" gid is not specified ");
        }
        int gid = arguments.gid;
        if(gid < 1 || gid > 0xFFFF ) {
            throw new IllegalArgumentException(" gid value "+gid+
            " is not in the range [1,65535]");
        }

        if(arguments.readOnly == null) {
            throw new IllegalArgumentException(" write flag (read-only|read-write) not specified");
        }
        boolean readOnly = arguments.readOnly.equals("read-only");

        if(arguments.home == null) {
            throw new IllegalArgumentException(" home is not specified ");
        }

        if(arguments.root == null) {
            throw new IllegalArgumentException(" root is not specified ");
        }

        if(arguments.arg1 == null) {
            throw new IllegalArgumentException(" user is not specified ");
        }
        String user = arguments.arg1;
        if(arguments.fsroot == null) {
            arguments.fsroot = arguments.root;
        }
        if(debug) {
            System.out.println(" adding user = "+user+
            " with uid = "+uid+
            ", gid = "+gid+
            ", home = "+arguments.home+
            ", root = "+arguments.root+
            ", fsroot = "+arguments.fsroot);

            if(arguments.passwd !=  null) {
                System.out.println(" password = "+arguments.passwd);
            }

            if(arguments.secureIds != null && !arguments.secureIds.isEmpty()) {
                System.out.println("secureIds are:");

                for (Object secureId1 : arguments.secureIds) {
                    String secureId = (String) secureId1;
                    System.out.println("\"" + secureId + "\"");
                }

                System.out.println();
            }
        }

        if(arguments.passwd !=  null) {
            if(pwd_records.containsKey(user)) {
                throw new IllegalArgumentException(" User "+ user +
                " already  has a password based authentication record");
            }
            UserPwdRecord pwd_record = new UserPwdRecord(user,arguments.passwd,readOnly,
            uid,gid,arguments.home, arguments.root,arguments.fsroot,true);
            pwd_records.put(user,pwd_record);
        }

        if(arguments.secureIds != null && !arguments.secureIds.isEmpty()) {
            if(auth_records.containsKey(user)) {
                throw new IllegalArgumentException(" User "+ user +
                " already  has an authentication record");
            }
            UserAuthRecord record = new UserAuthRecord(user,readOnly,uid,gid,arguments.home,
            arguments.root,arguments.fsroot,arguments.secureIds);
            auth_records.put(user,record);
        }
    }

    public void dcusermod(Arguments arguments) {
        if(arguments.arg1 == null) {
            throw new IllegalArgumentException(" user is not specified ");
        }
        String user = arguments.arg1;
        UserPwdRecord pwd_record = pwd_records.get(user);
        UserAuthRecord auth_record = auth_records.get(user);

        if( arguments.uid != null  ) {
            int uid = arguments.uid;
            if(uid < 1 || uid > 0xFFFF ) {
                throw new IllegalArgumentException(" uid value "+uid+
                " is not in the range [1,65535]");
            }
            if(pwd_record != null) {
                pwd_record.UID = uid;
            }
            if(auth_record != null) {
                auth_record.UID = uid;
            }
        }
        if( arguments.gid != null  ) {
            int gid = arguments.gid;
            if(gid < 1 || gid > 0xFFFF ) {
                throw new IllegalArgumentException(" gid value "+gid+
                " is not in the range [1,65535]");
            }
            if(pwd_record != null) {
                pwd_record.GID = gid;
            }
            if(auth_record != null) {
                auth_record.GID = gid;
            }
        }

        if(arguments.home != null) {
            if(pwd_record != null) {
                pwd_record.Home = arguments.home;
            }
            if(auth_record != null) {
                auth_record.Home = arguments.home;
            }
        }

        if(arguments.root != null) {
            if(pwd_record != null) {
                pwd_record.Root = arguments.root;
            }
            if(auth_record != null) {
                auth_record.Root = arguments.root;
            }
        }

        if(arguments.fsroot != null) {
            if(pwd_record != null) {
                pwd_record.FsRoot = arguments.fsroot;
            }
            if(auth_record != null) {
                auth_record.FsRoot = arguments.fsroot;
            }
        }

        if(arguments.passwd !=  null ) {
            if(pwd_record == null) {
                throw new IllegalArgumentException(" can not change password,"+
                " password based authentication record,"+
                " record for the user "+user+" does not exists");
            }
            pwd_record.setPassword(arguments.passwd);
        }

        if(arguments.disable) {
            if(pwd_record != null) {
                pwd_record.disable();
            }
        }

        if(arguments.secureIds != null && !arguments.secureIds.isEmpty()) {
            if(auth_record == null) {
                throw new IllegalArgumentException(" can not add secure ids to"+
                " the  authentication record,"+
                " record for the user "+user+" does not exists");
            }
            auth_record.addSecureIdentities(arguments.secureIds);
        }

        if(arguments.secureIds != null && !arguments.removeSecureIds.isEmpty()) {
            if(auth_record == null) {
                throw new IllegalArgumentException(" can not add secure ids to"+
                " the  authentication record,"+
                " record for the user "+user+" does not exists");
            }
            auth_record.removeSecureIdentities(arguments.removeSecureIds);
        }

        if(arguments.readOnly != null)
        {
            boolean readOnly=arguments.readOnly.equals("read-only");
            if(auth_record != null)
            {
                auth_record.ReadOnly = readOnly;
            }
            if(pwd_record != null)
            {
                pwd_record.ReadOnly = readOnly;
            }
        }



        if(debug) {
            System.out.println(" modifying user = "+user+
            " with uid = "+arguments.uid+
            ", gid = "+arguments.gid+
            ", home = "+arguments.home+
            ", root = "+arguments.root+
            ", fsroot = "+arguments.fsroot);

            if(arguments.passwd !=  null) {
                System.out.println(" password = "+arguments.passwd);
            }

            if(arguments.secureIds != null && !arguments.secureIds.isEmpty()) {
                System.out.println("secureIds are:");

                for (Object secureId1 : arguments.secureIds) {
                    String secureId = (String) secureId1;
                    System.out.println("\"" + secureId + "\"");
                }

                System.out.println();
            }
            if(pwd_record != null) {
                if(debug) {
                    System.out.println("new pwd record is :\n"+pwd_record);
                }
                pwd_records.put(user,pwd_record);
            }
            if(auth_record != null) {
                if(debug) {
                    System.out.println("new pwd record is :\n"+auth_record);
                }
                auth_records.put(user,auth_record);
            }
        }


        if(pwd_record != null) {
            pwd_records.put(user,pwd_record);
        }
        if(auth_record != null) {
            auth_records.put(user,auth_record);
        }


    }

    public void dcuserdel(Arguments arguments) {
        if(arguments.arg1 == null) {
            throw new IllegalArgumentException(" user is not specified ");
        }
        String user = arguments.arg1;
        UserPwdRecord pwd_record = pwd_records.remove(user);
        UserAuthRecord auth_record = auth_records.remove(user);
        if(pwd_record == null && auth_record == null) {
            throw new IllegalArgumentException("can not delete user "+user+
            ", user is not found");
        }
        if(debug) {
            System.out.println("removing user "+user);
            if(pwd_record == null) {
                System.out.println("removed password record "+pwd_record);
            }

            if(auth_record == null) {
                System.out.println("removed auth record "+auth_record);
            }

        }
    }

    public void dcuserlist(Arguments arguments) {
        String user = arguments.arg1;
        if(user != null) {
            UserPwdRecord pwd_record = pwd_records.get(user);
            if(pwd_record != null) {
                System.out.println(pwd_record.toDetailedString());
            }
            UserAuthRecord auth_record = auth_records.get(user);
            if(auth_record != null) {
                System.out.println(auth_record.toDetailedString());
            }
            return;
        }
        Set<String> allusers = new HashSet<>();
        allusers.addAll( pwd_records.keySet());
        allusers.addAll(auth_records.keySet());
        for (Object alluser : allusers) {
            System.out.println(alluser);
        }
    }

    public void dcmapadd(Arguments arguments) {
        if(arguments.arg1 == null ) {
            throw new IllegalArgumentException(" secureId is not specified ");
        }
        if(arguments.arg2 == null ) {
            throw new IllegalArgumentException(" user is not specified ");
        }
        String secureId=arguments.arg1;
        String user = arguments.arg2;
        if(mappings.containsKey(secureId)) {
            throw new IllegalArgumentException("can not add mapping for secureId \""
            +secureId+"\", it is already mapped to the user "+user);
        }
        mappings.put(secureId,user);
    }

    public void dcmapmod(Arguments arguments) {
        if(arguments.arg1 == null ) {
            throw new IllegalArgumentException(" secureId is not specified ");
        }
        if(arguments.arg2 == null ) {
            throw new IllegalArgumentException(" user is not specified ");
        }
        String secureId=arguments.arg1;
        String user = arguments.arg2;
        if( !mappings.containsKey(secureId) ) {
            throw new IllegalArgumentException("can not modify mapping for secureId \""
            +secureId+"\", secureId mapping is not found");
        }
        mappings.put(secureId,user);
    }

    public void dcmapdel(Arguments arguments) {
        if(arguments.arg1 == null ) {
            throw new IllegalArgumentException(" secureId is not specified ");
        }
        String secureId=arguments.arg1;
        if( !mappings.containsKey(secureId) ) {
            throw new IllegalArgumentException("can not delete mapping for secureId \""
            +secureId+"\", secureId mapping is not found");
        }
        mappings.remove(secureId);
    }

    public void dcmaplist(Arguments arguments) {
        String secureId=arguments.arg1;
        if(secureId != null) {
            if( !mappings.containsKey(secureId) ) {
                throw new IllegalArgumentException("can not find mapping for secureId \""+
                secureId+"\"");
            }
            System.out.println(" SecureId \""+secureId+"\" is mapped to a user "+
            mappings.get(secureId)+"\n");
            return;
        }
        for (Object o : mappings.keySet()) {
            secureId = (String) o;
            System.out
                    .println(" SecureId \"" + secureId + "\" is mapped to a user " +
                            mappings.get(secureId) + "\n");
        }
    }

    public void dcmappedtolist(Arguments arguments) {
        String theuser=arguments.arg1;
        if(theuser == null) {
            throw new IllegalArgumentException("user is not specified");
        }
        for (Object o : mappings.keySet()) {
            String secureId = (String) o;
            String user = mappings.get(secureId);
            if (theuser.equals(user)) {
                System.out.println("\"" + secureId + "\"");
            }
        }
    }


    public static class Arguments {
        public String command;
        String file;
        String arg1;
        String arg2;
        String readOnly;
        Integer uid;
        Integer gid;
        String home;
        String root;
        String fsroot;
        String passwd;
        boolean disable;
        boolean help;
        boolean debug;
        HashSet<String> secureIds= new HashSet<>();
        Collection<String> removeSecureIds = new HashSet<>();
    }

    public static Arguments parseArgs(String[] args, Arguments arguments) {
        if(args == null || args.length == 0) {
            throw new IllegalArgumentException("no arguments were specified");
        }
        int len = args.length;

/*        {
            System.out.println("parsing arguments:");
            for ( int i = 1; i < len; ++i )
            {
                System.out.println("args["+i+"] ="+args[i]);
            }
        }
 */
        if(arguments == null) {
            arguments = new Arguments();
        }
        arguments.command = args[0];


        for ( int i = 1; i < len; ++i ) {
            if( args[i].equals("-debug") ) {
                debug = true;
                arguments.debug = true;
            }
            else if( args[i].equals("-u") ) {
                arguments.uid = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("-g") ) {
                arguments.gid = Integer.parseInt(args[++i]);
            }
            else if( args[i].equals("-h") ) {
                arguments.home = args[++i];
            }
            else if( args[i].equals("-r") ) {
                arguments.root = args[++i];
            }
            else if( args[i].equals("-w") ) {
                arguments.readOnly = args[++i];
            }
            else if( args[i].equals("-f") ) {
                arguments.fsroot = args[++i];
            }
            else if( args[i].equals("-p") ) {
                arguments.passwd = args[++i];
            }
            else if( args[i].equals("-d") ) {
                arguments.disable = true;
            }
            else if(args[i].equals("-s")) {
                arguments.secureIds.add(args[++i]);
            }
            else if(args[i].equals("-sd")) {
                arguments.removeSecureIds.add(args[++i]);
            }
            else if(args[i].equalsIgnoreCase("-help") || args[i].equalsIgnoreCase("--help") ) {
                arguments.help = true;
            }
            else if(!args[i].startsWith("-")) {
                if(arguments.file == null) {
                    arguments.file = args[i];
                }
                else if(arguments.arg1 == null) {
                    arguments.arg1 = args[i];
                }
                else if(arguments.arg2 == null) {
                    arguments.arg2 = args[i];
                }
                else {
                    throw new IllegalArgumentException(" failed to parse argument  "+args[i] );
                }
            }
            else {
                throw new IllegalArgumentException(" failed to parse option  "+args[i] );
            }

        }
        return arguments;
    }

    private static final String header =
    "# This file was automatically generated by KAuthFile class\n"+
    "# Semiformal definition of the file format follows\n#\n"+
    "# The file has the following format:\n"+
    "# FILE = TOKENS\n"+
    "# TOKENS = TOKEN | TONENS NL TOKEN\n"+
    "# TOKEN = COMMENT | MAPPING | RECORD | PWDRECORD | EMPTYLINE \n"+
    "# NL =<new line symbol>\n"+
    "# WS = <any number of spaces or tabs> \n"+
    "# COMMENT = WS '#' "+
    "<any number of any symbols terminated by new line symbol>\n"+
    "# PWDRECORD =  WS "+
    PWD_RECORD_MARKER+
    " WS USER WS PASSWDHASH WS UID WS GID WS HOME WS ROOT WS [FSROOT WS]\n"+
    "# PASSWDHASH = <hash of password generated using a"+
    " crytografically strong hash function>\n"+
    "# PWDRECORD =  WS USER WS PASSWDHASH WS UID WS GID WS HOME WS ROOT WS [FSROOT WS]\n"+
    "# RECORD = USERAUTHENTICATION [SECUREIDS] EMPTYLINE \n"+
    "# USERAUTHENTICATION = WS "+
    AUTH_RECORD_MARKER+
    " WS USER WS UID WS GID WS HOME WS ROOT WS [FSROOT WS]\n"+
    "# USER = <username (no white spaces allowed)> \n"+
    "# UID = <integer> \n"+
    "# GID = <integer> \n"+
    "# HOME = <fully qualified unix path> \n"+
    "# ROOT = <fully qualified unix path> \n"+
    "# FSROOT = <fully qualified unix path> \n"+
    "# SECUREIDS = SECUREIDS ([COMMENT] |[SECUREIDLINE]) NL \n"+
    "# SECUREIDLINE = WS SECUREID WS NL\n"+
    "# SECUREID = <kerberos principal>|<grid identity (DN from x509 cert)>\n"+
    "# EMPTYLINE = WS NL\n"+
    "# MAPPING = WS "+
    MAPPING_MARKER+
    " WS <double quote> SECUREID <double quote> "+
    "USER NL\n"+
    "# \n\n";

    private static final String mapping_section_header =
    "# the following are the mappings from secure credetials ids to user names\n"+
    "# these are used to map credentials to the default user, \n"+
    "# if user is not supplied and can not be derived from credentials\n"+
    "# in user created files this do not have to be in a separate section\n\n";

    public static final String usage =
    " Usage [java -cp CLASSPATH org.dcache.srm.unixfs.KAuthFile] command [file] [-debug] [command arguments]\n"+
    "    where command is one of the following:\n"+
    "    dclist, convert, dcuserlist, dcuseradd, dcusermod, dcuserdel,\n"+
    "    dcmaplist, dcmappedtolist, dcmapadd, dcmapmod, dcmapdel. \n"+
    "    to get detailed descrition of commands give -help as a command argument \n"+
    "    each command must name must be followed by the kpwd file name if invoking \n"+
    "    from command line using java vm directly\n"+
    "    since invocation scripts do this automatically, file name is skipped in \n"+
    "    command specific usage messages\n";

    public  static final  String dclist_usage =
    " Usage: dclist [-debug] [-help] reads kpwd data and prints\n"+
    "         the data on standard out in a format suitable for kpwd file\n";

    public  static final String convert_usage =
    " Usage: convert [-debug] [-help] [file] reads from file or stdin in old format\n"+
    "         and prints the data in the new format\n";

    public  static final String dcuseradd_usage =
    " Usage: dcuseradd [-debug] [-help] -u uid -g gid -h home -r root -f fsroot -w read-access [-d] [-p passwd]\n"+
    "         [-s secureId1 [-s secureId2 [...[-s secreIdN]]]] user\n"+
    "         where passwd is the password to be used for weak authentication \n"+
    "         if read-access is \"read-only\" string then user granted the rights to \n"+
    "         read files only, otherwise, if read-access is any other string , then \n"+
    "         user is granted writest to both read and write files \n"+
    "         regular unix permissions still apply to all files \n"+
    "         and secureId is ether kerberos principal \n"+
    "         or x509 certificate Destinguised Name (DN),\n"+
    "         if secureId contains white spaces, enclose it in double quotes (\")\n";

    public  static final String dcusermod_usage =
    " Usage: dcusermod [-debug] [-help] [-u uid] [-g gid] [-h home] [-r root] [-f fsroot] [-w read-access] [-p passwd]\n"+
    "         [-s addSecureId1 [-s addSecureId2 [...[-s addSecureIdN]]]]\n"+
    "         [-sd removeSecureId1 [-sd removeSecureId2 [...[-sd removeSecureIdN]]]] user\n"+
    "         where passwd is the password to be used for weak authentication \n"+
    "         if read-access is \"read-only\" string then user granted the rights to \n"+
    "         read files only, otherwise, if read-access is any other string , then \n"+
    "         user is granted writest to both read and write files \n"+
    "         regular unix permissions still apply to all files \n"+
    "         and addSecureIds and removeSecureIds are ether kerberos principals\n"+
    "         or X509 certificate Destinguised Name (DN), \n"+
    "         if secureId contains white spaces, enclose it in double quotes (\")\n";

    public  static final String dcuserdel_usage =
    " Usage: dcuserdel [-debug] [-help] user\n";

    public  static final String dcuserlist_usage =
    " Usage: dcuserlist [-debug] [-help] [user]\n"+
    "         if user is not specified all users (with no details) are listed\n"+
    "         if user is specified user details are printed to the screen\n";

    public  static final String dcmapadd_usage =
    " Usage: dcmapadd [-debug] [-help] \"secureId\" user\n"+
    "         where secureId is either kerberos principaln"+
    "         or X509 certificate Destinguised Name (DN)\n";

    public  static final String dcmapmod_usage =
    " Usage: dcmapmod [-debug] [-help] \"secureId\" user\n"+
    "         where secureId is either kerberos principaln"+
    "         or X509 certificate Destinguised Name (DN)\n";

    public  static final String dcmapdel_usage =
    " Usage: dcmapdel [-debug] [-help] \"secureId\"\n"+
    "         where secureId is either kerberos principaln"+
    "         or X509 certificate Destinguised Name (DN)\n";

    public  static final String dcmaplist_usage =
    " Usage: dcmaplist [-debug] [-help] [ \"secureId\"]\n"+
    "         where secureId is either kerberos principaln"+
    "         or X509 certificate Destinguised Name (DN)\n"+
    "         if secureId is not specified all mappings are listed\n";

    public  static final String dcmappedtolist_usage =
    " Usage: dcmappedtolist [-debug] [-help] user\n"+
    "         where secureId is either kerberos principaln"+
    "         or X509 certificate Destinguised Name (DN)\n"+
    "         all secureIds that are mapped to the given user are listed\n";

    // it seams java does it automatically so unwrapping is not needed
    /*
    public static String unwrap(String s)
    {
        if(s.startsWith("\"") && s.endsWith("\""))
        {
            s = s.substring ( 1, s.length()-1 );
        }
        else if(s.startsWith("'") && s.endsWith("'"))
        {
            s = s.substring ( 1, s.length()-1 );
        }
        return s.trim();

    }*/

}




