/*
 * $Id: Pnfs2Himera.java 431 2008-01-10 08:18:17Z tigran $
 */
package org.dcache.chimera.migration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.dcache.chimera.DbConnectionInfo;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.JdbcFs;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.XMLconfig;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.util.SqlHelper;

public class Pnfs2Chimera {

    private static void convertDirectory(Connection mappingdbConnection,  FsInode dirInode, File dir, JdbcFs fs) {

        String[] list = dir.list();
        if (list == null)
            return;
        for (int i = 0, n = list.length; i < n; i++) {

            File f = new File(dir, list[i]);
            try {

                boolean isDirectory = f.isDirectory();
                String pnfsId = getId(f);

                Stat stat = getStat(dir, pnfsId);

                FsInode newInode = null;

                boolean isLink = (stat.getMode() & UnixPermission.S_IFLNK) == UnixPermission.S_IFLNK;
                if (isLink) {
                    String link = "/etc/profile";
                    newInode = fs.createLink(dirInode, f.getName(), stat
                            .getUid(), stat.getGid(), stat.getMode(), link
                            .getBytes());
                    fs.setFileSize(newInode, link.length());
                } else {

                    if (isDirectory) {
                        newInode = fs.mkdir(dirInode, f.getName(), stat
                                .getUid(), stat.getGid(), stat.getMode());

                        Map<String, String> tags = getPTags(f, pnfsId);
                        for(Map.Entry<String, String> tag : tags.entrySet() ) {

                            String tagName = tag.getKey();
                            String tagId = tag.getValue();

                            if( isPrimaryTag(f, tagId)) {
                                try {
                                    fs.statTag(newInode, tagName);
                                }catch(ChimeraFsException tnf) {
                                    fs.createTag(newInode, tagName);
                                }

                                byte [] tagData = getTag(f, tagName);
                                fs.setTag(newInode, tagName, tagData, 0, tagData.length);
                            }

                        }

                    } else {
                        newInode = fs.createFile(dirInode, f.getName(), stat
                                .getUid(), stat.getGid(), stat.getMode());
                        fs.setFileSize(newInode, f.length());

                        addMapping(mappingdbConnection, pnfsId, newInode.toString());

                        for( int level = 1; level < 7; level++) {
                        	File levelFile = new File(dir, ".(use)("+level+")("+list[i]+")");
                        	/*
                        	 * Opposite to Chimera, pnfs levels always exist
                        	 * migrate non empty levels only
                        	 */
                        	if(levelFile.length() > 0) {

                        		byte[] levelData = dumpLevel(levelFile);
                        		FsInode levelStore = fs.createFileLevel(newInode, level);

                        		levelStore.write(0, levelData, 0, levelData.length);
                        	}

                        }

                    }

                }

                if (stat.getATime() > 0) {
                    fs.setFileATime(newInode, stat.getATime());
                }

                if (stat.getMTime() > 0) {
                    fs.setFileMTime(newInode, stat.getMTime());
                }

                if (stat.getCTime() > 0) {
                    fs.setFileCTime(newInode, stat.getCTime());
                }

                StringBuilder sb = new StringBuilder();
                sb.append(pnfsId).append(" ").append(isDirectory ? "d " : "f ")
                        .append(f.toString()).append(" ").append(f.length())
                        .append(" ").append(getAttributes(f, pnfsId)).append(
                                " ");

                if (!isDirectory) {
                    for (int j = 1; j < 8; j++) {
                        byte[] x = readLevel(f, j);
                        if (x == null)
                            sb.append("-");
                        else
                            sb.append(Base64.byteArrayToBase64(x));
                        sb.append(" ");
                    }
                }

                // System.out.println(sb.toString()) ;
                if (f.isDirectory() && !isLink)
                    convertDirectory(mappingdbConnection, newInode, f, fs);

            } catch (Exception linee) {
                linee.printStackTrace();
                System.err.println("Problem scanning : " + f + " ("
                        + linee.getMessage() + ")");
            }

        }
    }

    /**
     * read content of level file
     *
     * @param levelFile
     * @return
     * @throws IOException
     */
    private static byte[] dumpLevel(File levelFile) throws IOException {


    	byte[] data = new byte[(int)levelFile.length()];

    	FileChannel fc = null;

    	try {

    		fc = new FileInputStream(levelFile).getChannel();

    		ByteBuffer bb = ByteBuffer.wrap(data);

    		bb.rewind();
    		fc.read(bb);

    	}finally{
    		if( fc != null) {
    			try { fc.close(); } catch(IOException ignored) { /* take it easy */}
    		}
    	}


		return data;
	}

	private static byte[] readLevel(File f, int level) throws IOException {

        File parent = f.getParentFile();
        String base = f.getName();

        File file = new File(parent, ".(use)(" + level + ")(" + base + ")");

        int len = (int) file.length();
        if (len == 0)
            return null;

        byte[] result = new byte[len];

        FileInputStream in = new FileInputStream(file);

        try {
            in.read(result, 0, result.length);
        } finally {
            try {
                in.close();
            } catch (IOException eee) {
                // ignored
            }
        }
        return result;
    }

    private static Stat  getStat(File file, String pnfsId) throws IOException {

        Stat stat = new Stat();

        File parent = file.getParentFile();
        File idFile = new File(parent, ".(getattr)(" + pnfsId + ")");

        BufferedReader br = new BufferedReader(new FileReader(idFile));
        String line = null;

        try {

            line = br.readLine();
            if( line == null) {
                throw new IOException("Failed to get file attributes of " + pnfsId);
            }
            String[] fields = line.split(":");

            stat.setMode(Integer.parseInt(fields[0], 8));
            stat.setUid(Integer.parseInt(fields[1]));
            stat.setGid(Integer.parseInt(fields[2]));
            stat.setATime(Long.parseLong(fields[3], 16)*1000);
            stat.setMTime(Long.parseLong(fields[4], 16)*1000);
            stat.setCTime(Long.parseLong(fields[5], 16)*1000);

        } finally {
            try {
                br.close();
            } catch (IOException ce) {
                // ignored
            }
        }

        return stat;
    }

    private static String getAttributes(File file, String pnfsId) throws IOException {

        File parent = file.getParentFile();
        File idFile = new File(parent, ".(getattr)(" + pnfsId + ")");

        BufferedReader br = new BufferedReader(new FileReader(idFile));
        String line = null;
        StringBuilder sb = new StringBuilder();
        try {
            sb.append(br.readLine());
            while ((line = br.readLine()) != null) {
                sb.append("/").append(line);
            }

        } finally {
            try {
                br.close();
            } catch (IOException ce) {
                // ignored
            }
        }

        return sb.toString();
    }

    private static String getId(File file) throws IOException {

        File parent = file.getParentFile();
        String base = file.getName();
        File idFile = new File(parent, ".(id)(" + base + ")");

        BufferedReader br = new BufferedReader(new FileReader(idFile));
        String line = null;
        try {
            line = br.readLine();
        } finally {
            try {
                br.close();
            } catch (IOException ce) {
                // ignored
            }
        }
        if (line == null)
            throw new IOException("Couldn't get pnfsId of " + file);

        return line;
    }

    /**
     *
     * @param dir
     * @param pnfsId
     * @return a hash map where key is the tag name and value is the id of the
     *         tag
     */
    public static Map<String, String> getPTags(File dir, String pnfsId) {

        File f = new File(dir, ".(ptags)(" + pnfsId + ")");
        BufferedReader br = null;
        String line = null, id = null, name = null;
        StringTokenizer st = null;
        Map<String, String> ptag = new HashMap<String, String>();

        try {
            br = new BufferedReader(new FileReader(f), 512);
            while ((line = br.readLine()) != null) {
                st = new StringTokenizer(line);
                try {
                    id = st.nextToken();
                    name = st.nextToken();
                } catch (Exception we) {
                    continue;
                }
                ptag.put(name, id);
            }
        } catch (Exception e) {
            /* take it easy */
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException ee) {/* to late to react */
                }
        }

        return ptag;
    }

    /**
     *
     * @param dir
     * @param tagId
     * @return true if tag is primary for a directory or false if tag is inherited
     */
    public static boolean isPrimaryTag( File dir, String tagId) {

        boolean isPrimary = false;

        File f = new File(dir, ".(showid)(" + tagId + ")");

        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(f), 512);
            String line = null;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if(line.startsWith("Flags")) {
                    String[] inheritedFlag = line.split(":");
                    if(inheritedFlag.length == 1 ) {
                        isPrimary = true;
                    }else if (inheritedFlag[1] != null && inheritedFlag[1].equals("inherited")) {
                        isPrimary = false;
                    }

                }
            }
        } catch (Exception e) {
            /* take it easy */
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException ee) {/* to late to react */
                }
        }

        return isPrimary;
    }


    private static byte[] getTag(File dir, String tagName ){

        File f = new File( dir , ".(tag)("+tagName+")" ) ;

        byte tagData[] = new byte[(int)f.length()];

        InputStream is = null ;
        try{
            is = new FileInputStream(f);
            is.read(tagData);
        }catch( IOException ee ){
            System.err.println("ioerror : " + ee);
           return null ;
        }finally{
            if(is != null) try{  is.close() ; }catch(IOException ee ){/* to late to react */}
        }
        return tagData;
     }


    private static void addMapping(Connection mappingdbConnection, String pnfsid, String chimeraid ) throws SQLException {

        PreparedStatement ps = mappingdbConnection.prepareStatement("INSERT INTO t_pnfsid_mapping VALUES (?,?)");

        try {
            ps.setString(1, pnfsid);
            ps.setString(2, chimeraid);
            ps.executeUpdate();
        }finally {
            SqlHelper.tryToClose(ps);
        }

    }

    public static void main(String[] args) throws Exception {


        if (args.length != 3) {
            System.err.println("Usage :" + Pnfs2Chimera.class.getName()
                    + " <chimera.config> <pnfs path> <chimera path>");
            System.exit(4);
        }

        XMLconfig config = new XMLconfig(new File(args[0]));

        JdbcFs fs = new JdbcFs(config);


        DbConnectionInfo cInfo = config.getDbInfo(0);
        Connection mappingdbConnection = DriverManager.getConnection(cInfo.getDBurl(), cInfo.getDBuser(), cInfo.getDBpass());
        mappingdbConnection.setAutoCommit(true);

        File dir = new File(args[1]);
        FsInode root = fs.path2inode(args[2]);

        Stat stat = getStat(dir, getId(dir));
        stat.setSize(512);
        stat.setMode(stat.getMode() | UnixPermission.S_IFDIR);

        FsInode dirInode = fs.mkdir(root, dir.getName(), stat.getUid(), stat.getGid(), stat
                .getMode());

        Pnfs2Chimera.convertDirectory(mappingdbConnection, dirInode, dir, fs);


    }

}
