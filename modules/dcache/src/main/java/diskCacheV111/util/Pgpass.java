//
// $Id: Pgpass.java,v 1.2 2005-08-19 23:45:26 timur Exp $
//

package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

/**
 *
 * @author  Vladimir Podstavkov
 */
public class Pgpass {

    private final String _pwdfile;
    private String _hostname;
    private String _port;
    private String _database;
    private String _username;

    public Pgpass(String pwdfile) {
        _pwdfile = pwdfile;
    }

    private String process(String line, String hostname, String port, String database, String username) {
        if (line.charAt(0) != '#') {
//         System.out.println("process: "+line);
            String[] sa = line.split(":");
//         for (int i = 0; i < sa.length; i++) {
//             System.out.print(sa[i]+",");
//         }
//         System.out.println();
            if (sa[0].equals("*") || sa[0].equals(hostname)) {
                if (sa[1].equals("*") || sa[1].equals(port)) {
                    if (sa[2].equals("*") || sa[2].equals(database)) {
                        if (sa[3].equals("*") || sa[3].equals(username)) {
                            return sa[4];
                        }
                    }
                }
            }
        }
        return null;
    }

    private boolean parseUrl(String url) {
        // -jdbcUrl=jdbc:postgresql:database
        // -jdbcUrl=jdbc:postgresql://host/database
        // -jdbcUrl=jdbc:postgresql://host:port/database
        String[] r = url.split("/");
        _hostname = "localhost";
        _port = "5432";
        if (r.length==1) {
            String[] r1 = r[0].split(":");
            _database = r1[r1.length-1];
        } else if (r.length==4) {
            _database = r[r.length-1];
            String[] r1 = r[2].split(":");
            _hostname = r1[0];
            if (r1.length==2) {
                _port = r1[1];
            } else if (r1.length > 2) {
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    public String getPgpass(String hostname, String port, String database, String username) {
        String result;

        if (!checkIfFileExists()) {
            System.out.println("File '" + _pwdfile + "' not exist");
            return null;
        }

        try {
            if (checkPgFilePermissions("rw-------")) {
                result = parsePgFile(hostname, port, database, username);

            } else {
                System.out.println("Protection for '" + _pwdfile + "' must be '600'");
                return null;

            }
        } catch (IOException e) {
            System.out.println("'" + _pwdfile + "': I/O error");
            return null;
        }

        return result;
    }

    /**
     * Check if the pwd file exists
     *
     * @return exist? then true
     */
    private boolean checkIfFileExists() {
        return new File(_pwdfile).isFile();
    }

    /**
     * Check the pwd file for selectable permissions. The file must be a POSIX file, at the moment.
     *
     * @param referencePermissionInput The permissions the file should have. It's in the unix like format.
     *                                 e.g. "rwx------" for owner read-write-execute
     * @return permissions right? true
     * @throws IOException
     */
    private boolean checkPgFilePermissions(String referencePermissionInput) throws IOException {

        if(checkIfOsIsPosixCompliant()){
            return checkPgFilePermissionsForPosix(referencePermissionInput);

        } else {
            System.out.println("Error reading permissions for '" + _pwdfile + "'. OS is not POSIX compliant");
            return false;
        }
    }

    /**
     * Check if the OS is POSIX compliant.
     *
     * @return
     */
    private boolean checkIfOsIsPosixCompliant(){
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private String parsePgFile(String hostname, String port, String database, String username) throws IOException {
            BufferedReader in = new BufferedReader(new FileReader(_pwdfile));
            String line, r = null;
            while ((line = in.readLine()) != null && r == null) {
                r = process(line, hostname, port, database, username);
            }
            in.close();
            return r;
    }

    /**
     * Check the pwd file for selectable permissions. Explicit for POSIX conform operating systems.
     * The file must be a POSIX file.
     *
     * @param referencePermissionInput The permissions the file should have. It's in the unix like format.
     *                                 e.g. "rwx------" for owner read-write-execute
     * @return permissions right? true
     * @throws IOException
     */
    private boolean checkPgFilePermissionsForPosix(String referencePermissionInput) throws IOException {
        Path path = Paths.get(_pwdfile);
        Set<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(path);
        Set<PosixFilePermission> referencePermissions = PosixFilePermissions.fromString(referencePermissionInput);

        boolean result = filePermissions.equals(referencePermissions);
        return result;
    }

    public String getPgpass(String url, String username) {
        if (parseUrl(url)) {
            return getPgpass(_hostname, _port, _database, username);
        }
        return null;
    }

    public String getHostname() {
        return _hostname;
    }

    public String getPort() {
        return _port;
    }

    public String getDatabase() {
        return _database;
    }

    public static String getPassword(String file,
                                     String url, String user, String password) {
        if (file != null && !file.trim().isEmpty()) {
            Pgpass pgpass = new Pgpass(file);
            return pgpass.getPgpass(url, user);
        }
        return password;
    }
}
