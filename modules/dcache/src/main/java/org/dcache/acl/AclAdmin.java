package org.dcache.acl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellCommandListener;

import org.dcache.acl.enums.RsType;
import org.dcache.acl.parser.ACEParser;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.Args;
import org.dcache.vehicles.FileAttributes;

/**
 * This component provides administration tool to manage ACLs
 * (commands for setting and getting ACL of files/directories).
 *
 * There are two commands for administration ACLs of the files or directories:
 * (1) set ACL (+ means ALLOW, - means DENY) :
 *     setfacl <pnfsId|globalPath>  <subject>:<+|-><access_msk>[:<flags>] [ ... ]
 *     Example:
 *     setfacl /pnfs/desy.de/data/dteam/MyFile USER:3750:+rdx EVERYONE@:-w GROUP:4567:-rda
 * (2) get ACL of file or directory
 *     Examples:
 *     getfacl /pnfs/desy.de/data/dteam/MyFile
 *     getfacl 000062981E06479F4174A51AE9E3AE48FFC1
 *
 * @author Irina Kozlova
 * @version 16 Dec 2008
 *
 */
public class AclAdmin
    implements CellCommandListener
{
    public static final String DEFAULT_ADDRESS_MSK = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

    final static int ACE_MIN_format2 = 2;
    final static int ACE_MAX_format2 = 5;

    public static final String OPTION_HELP = "h";

    private static final Logger _logger =
        LoggerFactory.getLogger(AclAdmin.class);

    private NameSpaceProvider _nameSpaceProvider;

    public void setNameSpaceProvider(NameSpaceProvider provider)
    {
        _nameSpaceProvider = provider;
    }

     /**
      * Command for getting ACL
      * Examples :
      * getfacl /pnfs/desy.de/data/dteam/MyFile
      * getfacl 000062981E06479F4174A51AE9E3AE48FFC1
      *
      */

     public final static String hh_getfacl = "<pnfsId>|<globalPath>" +
            "# gets ACL of an object";

     public final static String fh_getfacl =
            "getfacl <pnfsId>|<globalPath> \n" +
            "Gets ACL of a resource (a file or directory) \n" +
            "which is defined by its pnfsId or globalPath. \n" +
            "        \n" +
            "Example: \n" +
            "    getfacl /pnfs/example.org/data/MyDir \n" +
            "    ACL: rsId = 00004EEFE7E59A3441198E7EB744B0D8BA54, rsType = DIR \n" +
            "    type = A, accessMsk = lfsD, who = USER, whoID = 12457 \n" +
            "    type = A, flags = f, accessMsk = lfd, who = USER, whoID = 87552 \n" +
            "    In extra format: \n" +
            "    USER:12457:+lfsD \n" +
            "    USER:87552:+lfd:f \n" +
            "       \n" +
            "The extra format is suitable for the setfacl command. \n" +
            "See the help for that command for more details.";

     public String ac_getfacl_$_1(Args args)
         throws ACLException, CacheException
    {
        if (args.argc() < 1) {
            throw new IllegalArgumentException("Usage: getfacl <pnfsId>|<globalPath>");
        }

        PnfsId pnfsId = getPnfsId(args.argv(0));
        FileAttributes attr =
            _nameSpaceProvider.getFileAttributes(Subjects.ROOT, pnfsId,
                                                 EnumSet.of(FileAttribute.ACL));
        ACL acl = attr.getAcl();
        if (acl != null) {
            return acl.toExtraFormat();
        } else {
            return "ACL for the object rsId = " + args.argv(0) + "  does not exist.";
        }
    }

    /**
    * Command for setting ACL
    *
    * ace_spec format: <who>[:<who_id>]:<+|-><access_msk>[:<flags>] (+ means ALLOW, - means DENY)
    *
    * ace_spec examples: USER:6750:+rlx:fd EVERYONE@:-w USER:3550:+rlwfx:do GROUP:4352:+rlwfx
    *
    */
    public final static String hh_setfacl =
        "<pnfsId|globalPath>  <subject>:<+|-><access_msk>[:<flags>] [ ... ] \n" +
        "# sets a new ACL to an object <pnfsId|globalPath>";

    public final static String fh_setfacl =
            "setfacl <ID> <ACE> [<ACE> ...] \n" +
            "where \n"+
            "<ID>  is <pnfsId|globalPath> \n" +
            "<ACE> is <subject>:<+|-><access_msk>[:<flags>] \n" +
            "       \n" +
            "Sets a new ACL consisting of one or more ACEs to a resource (a file or directory), \n" +
            "which is defined by its pnfsId or globalPath. \n" +
            "Each ACE defines permissions to access this resource \n" +
            "for a subject (a user or group of users). \n" +
            "ACEs are space delimited and are ordered by significance, i.e., \n" +
            "first match wins.\n" +
            "       \n" +
            "Description of the <ACE> structure. \n" +
            "       \n" +
            "The element <subject> defines the subject of the ACE and\n" +
            "must be one of the following values: \n" +
            " USER:<who_id> : user identified by the virtual user ID <who_id> \n" +
            "GROUP:<who_id> : group identified by the virtual group ID <who_id> \n" +
            "        OWNER@ : user who owns the resource\n" +
            "        GROUP@ : group that owns the resource \n" +
            "     EVERYONE@ : world, including the owner and owning group\n" +
            "    ANONYMOUS@ : accessed without any authentication \n" +
            "AUTHENTICATED@ : any authenticated user (opposite of ANONYMOUS) \n" +
            "       \n" +
            "The element <access_msk> is a set of bits describing \n" +
            "how correspondent permissions will be modified for users matching the <subject>.\n" +
            "If <access_msk> is preceded by a '+' then corresponding operations are allowed.\n" +
            "If it is preceded by a '-' then corresponding operations are disallowed.\n " +
            "Some bits apply only if the <ID> is a file, some apply only if <ID> is a directory\n" +
            "and some to both. \n"+
            "If the <ID> is not the correct type then the bit is converted to appropriate one,\n" +
            "as indicated in parentheses. \n" +
            "The following access permissions may be used: \n" +
            "   r : Permission to read the data of a file (converted to 'l' if directory). \n" +
            "   l : Permission to list the contents of a directory (converted to 'r' if file). \n" +
            "   w : Permission to modify a file's data anywhere in the file's offset range.\n" +
            "       This includes the ability to write to any arbitrary offset and \n" +
            "       as a result to grow the file. (Converted to 'f' if directory).\n" +
            "   f : Permission to add a new file in a directory (converted to 'w' if file).\n" +
            "   a : The ability to modify a file's data, but only starting at EOF \n"+
            "       (converted to 's' if directory).\n" +
            "   s : Permission to create a subdirectory in a directory (converted to 'a' if file).\n" +
            "   x : Permission to execute a file or traverse a directory.\n" +
            "   d : Permission to delete the file or directory.\n" +
            "   D : Permission to delete a file or directory within a directory.\n" +
            "   n : Permission to read the named attributes of a file or to lookup \n" +
            "       the named attributes directory.\n" +
            "   N : Permission to write the named attributes of a file or \n" +
            "       to create a named attribute directory.\n" +
            "   t : The ability to read basic attributes (non-ACLs) of a file or directory.\n" +
            "   T : Permission to change the times associated with a file \n" +
            "       or directory to an arbitrary value.\n" +
            "   c : Permission to read the ACL.\n" +
            "   C : Permission to write the acl and mode attributes.\n" +
            "   o : Permission to write the owner and owner group attributes.\n" +
            "       \n" +
            "To enable ACL inheritance, the optional element <flags> must be defined.\n" +
            "Multiple bits may be specified as a simple concatenated list of letters.\n" +
            "Order doesn't matter. \n" +
            "   f : Can be placed on a directory and indicates that this ACE \n" +
            "       should be added to each new file created.\n" +
            "   d : Can be placed on a directory and indicates that this ACE \n" +
            "       should be added to each new directory created.\n" +
            "   o : Can be placed on a directory and indicates that this ACE \n" +
            "       should be ignored for this directory.\n" +
            "       Any ACE that inherit from an ACE with 'o' flag set will not have the 'o' flag set.\n" +
            "       Therefore, ACEs with this bit set take effect only if they are inherited \n" +
            "       by newly created files or directories as specified by the above two flags.\n" +
            "       REMARK: If 'o' flag is present on an ACE, then \n" +
            "       either 'd' or 'f' (or both) must be present as well.\n" +
            "       \n" +
            "Example: \n" +
            "setfacl /pnfs/example.org/data/TestDir USER:3750:+lfs:d EVERYONE@:+l GROUP:8745:-s USER:3452:+D \n" +
            "       Permissions for TestDir are altered so: \n" +
            "       First ACE. User with id 3750 (USER:3750) is allowed to \n"+
            "          list directory contents (l), \n" +
            "          create files (f), \n"+
            "          create subdirectories (s), \n" +
            "          and these permissions will be inherited by all newly created \n" +
            "          subdirectories as well (d). \n" +
            "       Second ACE. Everyone (EVERYONE@) is allowed to \n"+
            "          list directory contents. \n" +
            "       Third ACE. Group with id 8745 (GROUP:8745) is not allowed to \n" +
            "          create subdirectories.\n" +
            "       Fourth ACE. User with id 3452 (USER:3452) is allowed to \n" +
            "          delete objects within this directory (D). The user must also have \n" +
            "          the delete permission (d) for the object to be deleted. See next example.\n " +
            "       \n" +
            "setfacl /pnfs/example.org/data/TestDir/TestFile USER:3452:+d  \n" +
            "       Permissions for TestFile are altered so: \n" +
            "       First ACE. User with id 3452 (USER:3452) is allowed to \n" +
            "          delete this resource (d). To delete TestFile, the user must also \n"+
            "          have permission to delete directory contents (D). See previous example.\n" +
            "       \n" +
            "For further information on ACLs in dCache please refer to: \n" +
            "http://trac.dcache.org/trac.cgi/wiki/Integrate";

    public String ac_setfacl_$_2_99(Args args)
        throws CacheException, IllegalArgumentException
    {
        if (args.argc() < 2) {
            throw new IllegalArgumentException("Usage : setfacl <pnfsid|globalPath>  <who>[:<who_id>]:<+|-><access_msk>[:<flags>] [ ... ] ");
        }

        PnfsId pnfsId = getPnfsId(args.argv(0));
        RsType rsType = getRsType(pnfsId);

        // get list of ACEs
        List<ACE> aces = new ArrayList<>();

        for (int i = 1; i < args.argc(); i++) {
            String ace_spec_format2 = args.argv(i);

            ACE ace = ACEParser.parseAdmACE(ace_spec_format2);
            if ( ace == null ) {
                throw new IllegalArgumentException("Can't parse provided ACE: " + ace_spec_format2);
            }
            aces.add(ace);
        }

        FileAttributes attributes = new FileAttributes();
        attributes.setAcl(new ACL(rsType, aces));
        _nameSpaceProvider.setFileAttributes(Subjects.ROOT, pnfsId, attributes,
                EnumSet.noneOf(FileAttribute.class));
        return "Done";
    }

    private PnfsId getPnfsId(String s)
        throws CacheException
    {
        if (PnfsId.isValid(s)) {
            return new PnfsId(s);
        } else {
            return _nameSpaceProvider.pathToPnfsid(Subjects.ROOT, s, true);
        }
    }

    private RsType getRsType(PnfsId pnfsId)
        throws CacheException
    {
        FileAttributes attributes =
            _nameSpaceProvider.getFileAttributes(Subjects.ROOT, pnfsId,
                                                 EnumSet.of(FileAttribute.TYPE));
        switch (attributes.getFileType()) {
        case DIR:
            return RsType.DIR;
        case REGULAR:
            return RsType.FILE;
        }
        return null;
    }
}
