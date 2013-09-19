/*
 * DiskFileMetaData.java
 *
 * Created on July 28, 2004, 4:58 PM
 */

package org.dcache.srm.unixfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.StringTokenizer;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMUser;

/**
 *
 * @author  timur
 */
public class UnixfsFileMetaData extends FileMetaData{
    private static final long serialVersionUID = 5724147825388462210L;
    private static final Logger logger =  LoggerFactory.getLogger(UnixfsFileMetaData.class.getName());
    public boolean filo ;
    public boolean character_device ;
    public boolean directory ;
    public boolean block_device ;
    public boolean regular_file ;
    public boolean symbolic_link ;
    public boolean socket ;
    public static final int  S_IFMT    = 0170000 ;  //bitmask for the file type bitfields
    public static final int S_IFSOCK  = 0140000 ;  //socket
    public static final int S_IFLNK   = 0120000 ;  //symbolic link
    public static final int S_IFREG   = 0100000 ;  //regular file
    public static final int S_IFBLK   = 0060000 ;  //block device
    public static final int S_IFDIR   = 0040000 ;  //directory
    public static final int S_IFCHR   = 0020000 ;  //character device
    public static final int S_IFIFO   = 0010000 ;  //fifo
    public static final int S_ISUID   = 0004000 ;  //set UID bit
    public static final int S_ISGID   = 0002000 ;  //set GID bit (see below)
    public static final int S_ISVTX   = 0001000 ;  //sticky bit (see below)
    public static final int S_IRWXU   = 00700   ;  //mask for file owner permissions
    public static final int S_IRUSR   = 00400   ;  //owner has read permission
    public static final int S_IWUSR   = 00200   ;  //owner has write permission
    public static final int S_IXUSR   = 00100   ;  //owner has execute permission
    public static final int S_IRWXG   = 00070   ;  //mask for group permissions
    public static final int S_IRGRP   = 00040   ;  //group has read permission
    public static final int S_IWGRP   = 00020   ;  //group has write permission
    public static final int S_IXGRP   = 00010   ;  //group has execute permission
    public static final int S_IRWXO   = 00007   ;  //mask for permissions for others (not in group)
    public static final int S_IROTH   = 00004   ;  //others have read permission
    public static final int S_IWOTH   = 00002   ;  //others have write permisson
    public static final int S_IXOTH   = 00001   ;  //others have execute permission
    /* example of outputs:
     timur@fnisd1 scripts]$ stat -c "Acces=%a blocks=%b mode=%f group=%g user=%u links=%h inode=%i block_size=%o size=%s dev_min=%T dev_maj=%t" srm-stat
Acces=644 blocks=8 mode=81a4 group=1530 user=10401 links=1 inode=1173015 block_size=4096 size=1984 dev_min=14 dev_maj=2b
[timur@fnisd1 scripts]$ stat -t srm-stat
srm-stat 1984 8 81a4 10401 1530 303 1173015 1 2b 14 1091050797 1091050795 1091050795 4096
[timur@fnisd1 scripts]$
     *?
     */
    // name size blocks mode uid gid  ? inode hrd_links_num major_dev minor_dev access1 access2 access3
    /** Creates a new instance of DiskFileMetaData */
    public UnixfsFileMetaData(String path, String srm_host, int srm_port, String addler32, String stat_desh_t_output) {

        StringTokenizer st = new StringTokenizer(stat_desh_t_output);
        if(st.countTokens() <6) {
            throw new IllegalArgumentException("bad \"stat -t\" output, can not parse:\n"+stat_desh_t_output);
        }
        SURL = "srm://"+srm_host+":"+srm_port+"/"+path;
        String name = st.nextToken();
        size = new Long(st.nextToken()).intValue();
        long blocks = Long.parseLong(st.nextToken());
        int mode = Integer.parseInt(st.nextToken(),16);
        owner = st.nextToken();
        group = st.nextToken();

        int file_type = mode & S_IFMT;
        filo = (mode & S_IFIFO) != 0;
        character_device = (mode & S_IFCHR) != 0;
        directory = (mode & S_IFDIR) != 0;
        block_device = (mode & S_IFBLK) != 0;
        regular_file = (mode & S_IFIFO) != 0;
        symbolic_link = (mode & S_IFLNK) != 0;
        socket = (mode & S_IFSOCK) != 0;
        int permissions = mode & 0777;
        this.permMode = permissions;
        if(addler32 != null) {
            this.checksumType = "adler32";
            this.checksumValue = addler32;
        }

    }

    @Override
    public  boolean isOwner(SRMUser user) {
        try {
            return Integer.parseInt(owner) == ((UnixfsUser) user).getUid();
        } catch (NumberFormatException nfe) {
            logger.debug("owner is not a number: "+owner,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a UnixfsUser: "+user,cce);
            throw cce;
        }
    }

    @Override
    public boolean isGroupMember(SRMUser user) {
        try {
            return Integer.parseInt(group) == ((UnixfsUser) user).getUid();
        } catch (NumberFormatException nfe) {
            logger.debug("group is not a number: "+group,nfe);
            throw nfe;
        } catch (ClassCastException  cce) {
            logger.error("user is not a UnixfsUser: "+user,cce);
            throw cce;
        }
    }

}
