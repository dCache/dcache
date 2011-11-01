package diskCacheV111.util;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Set;
import java.util.EnumSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.namespace.provider.EmptyTrash;
import diskCacheV111.namespace.provider.Trash;

import org.dcache.namespace.FileType;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;
import static org.dcache.namespace.FileAttribute.*;

public class PnfsFile extends File
{
    private static final long serialVersionUID = -5470614764547228403L;

    private static Trash _trash = new EmptyTrash("empty trash");

    private static final Logger _logNameSpace =
        LoggerFactory.getLogger("logger.dev.org.dcache.namespace." + PnfsFile.class.getName());

    //
    // taken from linux stat man page
    //
    private static final int ST_FILE_FMT  = 0170000 ;
    private static final int ST_REGULAR   = 0100000 ;
    private static final int ST_DIRECTORY = 0040000 ;
    private static final int ST_SYMLINK   = 0120000 ;
    private static final int ST_PERMISSION = 00777;


    //
    // various constants for interpreting a .(getattr) line
    //
    private final static int MODE_INDEX = 0;
    private final static int UID_INDEX = 1;
    private final static int GID_INDEX = 2;
    private final static int ACCESS_TIME_INDEX = 3;
    private final static int MODIFICATION_TIME_INDEX = 4;
    private final static int CREATION_TIME_INDEX = 5;
    private final static int NUMBER_OF_FIELDS_IN_GETATTR = 6;

    private final static int ACCESS_TIME_FACTOR = 1000;
    private final static int MODIFICATION_TIME_FACTOR = 1000;
    private final static int CREATION_TIME_FACTOR = 1000;
    private final static int MODE_BASE = 8;
    private final static int UID_BASE = 10;
    private final static int GID_BASE = 10;
    private final static int ACCESS_TIME_BASE = 16;
    private final static int MODIFICATION_TIME_BASE = 16;
    private final static int CREATION_TIME_BASE = 16;

    public final static Set<FileAttribute> GETATTR_ATTRIBUTES =
        EnumSet.of(ACCESS_TIME, CREATION_TIME, MODIFICATION_TIME,
                   MODE, OWNER, OWNER_GROUP, SIMPLE_TYPE, TYPE);

    private PnfsId  _pnfsId;

    static private String getCanonicalPath(File f)
        throws CacheException
    {
       try {
           return f.getCanonicalPath();
       } catch (IOException e) {
           throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                    "Failed to resolve " + f + ": " + e.getMessage());
       }
    }

   public PnfsFile( String path ) throws CacheException {
       super(getCanonicalPath(new File(path)));
   }
   public PnfsFile( File path , String file ) throws CacheException {
       super(getCanonicalPath(new File(path , file)));
   }

   private PnfsFile( String mp , String file , PnfsId id ) throws CacheException {
       super(getCanonicalPath(new File(mp, file)));
       _pnfsId   = id ;
   }

    @Override
    public boolean exists()
    {
        if (_pnfsId != null) {
            try {
                FileAttributes attributes = new FileAttributes();
                readGetAttr(new File(getParent()), attributes);
                if (attributes.getFileType() == FileType.LINK) {
                    return true;
                }
            } catch (CacheException ex) {
                return false;
            } catch (IOException ex) {
                return false;
            }
        }
        return super.exists();
    }

    /**
     * Fills in the attributes that are available in the .(getattr)
     * file. Those are the attributes in GETATTR_ATTRIBUTES.
     */
    public void readGetAttr(File mp, FileAttributes attributes)
        throws CacheException, IOException
    {
        PnfsId pnfsId = getPnfsId();
        File metafile = new File(mp, ".(getattr)(" + pnfsId.getId() + ")");
        RandomAccessFile raf = new RandomAccessFile(metafile, "r");
        try {
            String line = raf.readLine();
            if (line == null) {
                throw new CacheException("Cannot read meta [" + pnfsId +"]");
            }

            try {
                String[] s = line.split(":");
                if (s.length < NUMBER_OF_FIELDS_IN_GETATTR) {
                    throw new IOException("Illegal meta data format : " +
                                          pnfsId + " (" + line + ")");
                }

                int mode =
                    Integer.parseInt(s[MODE_INDEX], MODE_BASE);
                int uid =
                    Integer.parseInt(s[UID_INDEX], UID_BASE);
                int gid =
                    Integer.parseInt(s[GID_INDEX], GID_BASE);
                long accessTime =
                    Long.parseLong(s[ACCESS_TIME_INDEX],
                                   ACCESS_TIME_BASE);
                long modificationTime =
                    Long.parseLong(s[MODIFICATION_TIME_INDEX],
                                   MODIFICATION_TIME_BASE);
                long creationTime =
                    Long.parseLong(s[CREATION_TIME_INDEX],
                                   CREATION_TIME_BASE);

                switch (mode & ST_FILE_FMT) {
                case ST_REGULAR:
                    attributes.setFileType(FileType.REGULAR);
                    break;
                case ST_DIRECTORY:
                    attributes.setFileType(FileType.DIR);
                    break;
                case ST_SYMLINK:
                    attributes.setFileType(FileType.LINK);
                    break;
                default:
                    attributes.setFileType(FileType.SPECIAL);
                    break;
                }

                attributes.setMode(mode & ST_PERMISSION);
                attributes.setOwner(uid);
                attributes.setGroup(gid);
                attributes.setAccessTime(accessTime * ACCESS_TIME_FACTOR);
                attributes.setModificationTime(modificationTime * MODIFICATION_TIME_FACTOR);
                attributes.setCreationTime(creationTime * CREATION_TIME_FACTOR);
            } catch (NumberFormatException e){
                throw new IOException("Illegal meta data format: " +
                                      pnfsId + " (" + line + ")");
            }
        } catch (FileNotFoundException e) {
            boolean deleted = PnfsFile.isDeleted(pnfsId);
            if (deleted) {
                throw new FileNotFoundCacheException("No such file or directory [" + pnfsId + "]");
            } else {
                throw new NotInTrashCacheException("Not in trash: " + pnfsId);
            }
        } finally {
            raf.close();
        }
    }

   public boolean isPnfs(){
      if( isDirectory() ){
         File f = new File( this , ".(const)(x)" ) ;
         return f.exists() ;
      }else if( isFile() ){
         String dirString = getParent() ;
         if( dirString == null )return false ;
         File f = new File( dirString , ".(const)(x)" ) ;
         return f.exists() ;
      }else return false ;
   }

   @Override
   public boolean delete(){
     if( _pnfsId == null )return super.delete() ;
     return false ;
   }

    /*
     * This method checks if the file with a name as given pnfsid exists in the trash directory
     * The result can be 'true/false'
     */
    public static boolean isDeleted(PnfsId pnfsId) {
        return _trash.isFound(pnfsId.getId());
    }

    public static void setTrash(Trash trash) {
        if (trash != null) {
            _trash = trash;
        } else {
            throw new IllegalArgumentException("'trash' is not defined.");
        }
    }

    public boolean setLength( long length ){

	   File setSizeCommand = null;

      if( _pnfsId == null ){
         if( ! isFile() )return false ;
         String dirString = getParent() ;
         if( dirString == null )return false  ;
         setSizeCommand =
         new File( dirString ,
                   ".(fset)("+getName()+")(size)("+length+")" ) ;

         if( length == 0L ){
            return true ;
         }else{

            try{
            	if(setSizeCommand.length() != 0L ){
            		setSizeCommand.delete();
            	}
            	setSizeCommand.createNewFile();
            	return length() == length ;
            }catch( IOException e ){
                return false ;
            }
         }
      }else{
         String parent = getParent() ;
         setSizeCommand = new File( parent , ".(pset)("+_pnfsId+")(size)("+length+")" ) ;
         if( length == 0 ){
             return true ;
         }else{
            try{

            	if(setSizeCommand.length() != 0L ){
            		setSizeCommand.delete();
            	}
            	setSizeCommand.createNewFile();

                return setSizeCommand.length() == length ;
            }catch( IOException ioe ){
                return false ;
            }
         }
      }
   }
   public File getLevelFile( int level ){
      if( ( level<=0) && ( level > 7 ) )
        throw new IllegalArgumentException( "Illegal Level "+level ) ;

      if( _pnfsId != null ){
	  return new File( getParent()+"/.(puse)("+_pnfsId+")("+level+")" ) ;
	  //         return new File( _absolute+"("+level+")" ) ;
      }else{
         String dirString = getParent() ;
         if( dirString == null )return null ;
         return new File( dirString , ".(use)("+level+")("+getName()+")" ) ;
      }
   }

    public PnfsId getPnfsId() throws CacheException
    {
        if (_pnfsId != null)
            return _pnfsId;

        if(!exists()) {
            throw new FileNotFoundCacheException("path " +this+" not found");
        }

        String dirString = getParent();
        if (dirString == null) {
            throw new FileNotFoundCacheException("path " +this+" not found");
        }

        File f = new File(dirString, ".(id)(" + getName() + ")");
        String idString;
        try {
            try {
                idString = readLine(f);
            } catch (FileNotFoundException e) {
                /* Some OSes have a negative directory entry cache. Since
                 * the .(id) file is created as a side effect of creating
                 * other files, we need to flush the cache. Attempting to
                 * create the file will achieve this. The operation always
                 * fails so we ignore the exception.
                 */
                try {
                    f.createNewFile();
                } catch (IOException ignored) {
                }

                /* Now try to read it again.
                 */
                try {
                    idString = readLine(f);
                } catch (FileNotFoundException ee) {
                    throw new FileNotFoundCacheException("File not found");
                }

                _logNameSpace.warn("First attempt to retrieve PNFS ID of " +
                                   this + " failed. Second attempt succeeded " +
                                   "after clearing negative cache entry. " +
                                   "This is caused by a known PNFS problem.");
            }
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Failed to read PNFS ID of file: " +
                                     e.getMessage());
        }

        if (idString == null) {
            throw new FileNotFoundCacheException("Path " +this+" not found (empty id file)");
        }
        return new PnfsId(idString);
    }

    private static String readLine(File file)
        throws IOException
    {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        try {
            return raf.readLine();
        } finally {
            try {
                raf.close();
            } catch (IOException e) {
                _logNameSpace.warn("Failed to close file [" + file + "]: ", e);
            }
        }
    }

   public String [] getTags(){
      if( _pnfsId == null ){
         return _getRealTags() ;
      }else{
         String [][] v  = getPTags() ;
         String []   ar = new String[v.length] ;
         for( int i = 0 ; i < v.length ; i++ )
             ar[i] = v[0][i] ;
         return ar ;
      }
   }
   public String [][] getPTags(){
      if( _pnfsId == null )
         throw new
         IllegalArgumentException( "Not a pFile" ) ;

      String mountpoint    = getParent() ;
      File            f    = new File( mountpoint ,
                                       ".(ptags)("+_pnfsId+")" ) ;
      BufferedReader  br   = null ;
      String          line = null , id = null , name = null ;
      StringTokenizer st   = null ;
      List<String[]>          v    = new ArrayList<String[]>() ;
      String []       pair = null ;
      try{
         br = new BufferedReader(  new FileReader( f ) ,512) ;
         while( ( line = br.readLine() )  != null ){
             st = new StringTokenizer(line) ;
             try{
                id   = new PnfsId( st.nextToken() ).toString() ;
                name = st.nextToken() ;
             }catch(Exception we ){ continue ;}
             pair = new String[2] ;
             pair[0] = name ;
             pair[1] = id ;
             v.add(pair) ;
         }
      }catch(IOException e){
          // FIXME: we return as much as we can get
      }finally{
    	  if(br != null) try{  br.close() ; }catch(IOException ee ){/* to late to react */}
      }
      String [][] sv =  new String[v.size()][] ;
      return v.toArray( sv ) ;
   }

   @Override
   public boolean isDirectory(){
      if( _pnfsId == null )return super.isDirectory() ;
      String type = getPnfsFileType() ;
      return ( type.charAt(2) == 'I' ) &&
             ( type.charAt(5) == 'd' )    ;
   }

   @Override
   public boolean isFile(){
      if( _pnfsId == null )return super.isFile() ;
      String type = getPnfsFileType() ;
      return ( type.charAt(2) == 'I' ) &&
             ( type.charAt(6) == 'r' )    ;
   }

   public boolean isLink(){
       if( _pnfsId == null )return super.isFile() ;
       String type = getPnfsFileType() ;
       return ( type.charAt(2) == 'I' ) &&
              ( type.charAt(7) == 'l' )    ;
    }
   public String getPnfsFileType(){
      if( _pnfsId == null )
         throw new
         IllegalArgumentException( "Not in P mode" ) ;

      File f = new File( getParent() , ".(showid)("+_pnfsId+")" ) ;
      BufferedReader  br   = null ;
      String          line = null , tmp = null ;
      StringTokenizer st   = null ;
      try{
         br = new BufferedReader(  new FileReader( f ),1024 ) ;
         while( ( line = br.readLine() )  != null ){
             st = new StringTokenizer(line) ;
             try{
                tmp = st.nextToken() ;
                if( ! tmp.equals( "Type" ) )continue ;
                tmp = st.nextToken() ;
                tmp = st.nextToken() ;
                if( tmp.length() != 11 )
                  throw new
                  Exception( "Illegal 'showid' format" );
                return tmp ;
             }catch(NoSuchElementException we ){ continue ;}
         }
      }catch(Exception e){
         return null ;
      }finally{
         if(br != null) try{  br.close() ; }catch(IOException ee ){/* to late to react */}
      }
      return null ;
   }
   private String [] _getRealTags(){

      if( ( ! isDirectory() ) || ( ! isPnfs() ) )return null ;
      File f = new File( this , ".(tags)(x)" ) ;
      if( ! f.exists() )return null ;
      List<String> v = new Vector<String>() ;
      BufferedReader r = null ;
      try{
         String line = null ;
         r = new BufferedReader( new FileReader( f ),64 ) ;
         while( ( line = r.readLine() ) != null ){
            v.add( line ) ;
         }
      }catch( IOException ee ){
         return null ;
      }finally{
         if(r != null) try{  r.close() ; }catch(IOException ee ){/* to late to react */}
      }
      String [] a = new String[v.size()] ;

      int j = 0 ;
      for( int i= 0 ; i < v.size() ; i ++ ){
         if( ( a[j] = tagNameOf( v.get(i) ) ) == null )
           continue ;
         j++ ;
      }
      if( j < v.size() ){
          String [] b = new String[j] ;
          System.arraycopy( a , 0 , b , 0 , j ) ;
          return b ;
      }
      return a ;
   }
   public static String pathfinder( File mountpoint , String pnfsId )
          throws IOException{

      List<String> v = new ArrayList<String>() ;
      while(true){
         try{
            v.add( _getNameOf( mountpoint , pnfsId ) ) ;
         }catch(IOException ioe ){
            break ;
         }
         pnfsId = _getParentId( mountpoint , pnfsId ) ;
      }
      if( v.size() == 0 )
        throw new
        IllegalArgumentException( "No path for "+pnfsId) ;

      StringBuilder sb = new StringBuilder() ;

      for( int i = v.size() - 1 ; i >= 0 ; i-- ){
          sb.append("/").append(v.get(i)) ;
      }

      return sb.toString() ;
   }
   public static String _getParentId( File mountpoint , String  pnfsId )
          throws IOException{

       BufferedReader br =
          new BufferedReader(
             new FileReader(
                new File( mountpoint , ".(parent)("+pnfsId+")" ) ),32 ) ;
       try{
          return br.readLine().trim() ;
       }finally{
          try{ br.close() ;}catch(IOException e){}
       }
   }
   public static String _getNameOf( File mountpoint , String  pnfsId )
          throws IOException{

       if (_logNameSpace.isInfoEnabled() ) {
           _logNameSpace.info("nameof for pnfsid " + pnfsId);
       }

       BufferedReader br =
          new BufferedReader(
             new FileReader(
                new File( mountpoint , ".(nameof)("+pnfsId+")" ) ),64 ) ;
       try{
          return br.readLine().trim() ;
       }finally{
          try{ br.close() ;}catch(IOException e){}
       }
   }
   public PnfsId getParentId() throws CacheException {
      PnfsId pnfsId = getPnfsId();

      if( pnfsId != null ){

          if (_logNameSpace.isInfoEnabled() ) {
              _logNameSpace.info("parent for pnfsid " + pnfsId);
          }

         File   f    = new File( getParent() , ".(parent)("+pnfsId+")" ) ;
         String line = null ;
         BufferedReader br = null ;
         try{
            br   = new BufferedReader( new FileReader( f ),32) ;
            line = br.readLine() ;
         }catch(IOException ioe ){

         }finally{
        	 if(br != null) try{  br.close() ; }catch(IOException ee ){/* to late to react */}
         }

         return line == null ? null : new PnfsId(line) ;
      }else{
         return null ;
      }
   }
   private String tagNameOf( String str ){
      if( ( ! str.startsWith( ".(tag)(" ) ) ||
          ( ! str.endsWith( ")" )         )    )return null ;
      return str.substring( 7 , str.length()-1 ) ;
   }
   public String [] getTag( String tagName ){
     return _pnfsId == null ? _getRealTag(tagName) :
                              _getPTag(tagName) ;

   }
   private String [] _getPTag( String tagName ){
       String [] [] tags = getPTags() ;
       int i = 0 ;
       for( i = 0 ;
            ( i < tags.length ) &&
            ( ! tags[i][0].equals(tagName) ) ; i++ ) ;
       if( i == tags.length )return null ;

       File f = new File( getParent() , ".(access)("+tags[i][1]+")" ) ;
       BufferedReader r = null ;
      try{
         List<String> v = new Vector<String>() ;
         String line = null ;
         r = new BufferedReader( new FileReader( f ),64 ) ;
         while( ( line = r.readLine() ) != null ){
            v.add( line ) ;
         }
         String [] a = new String[v.size()] ;

         return v.toArray(a);
      }catch( IOException ee ){
         return null ;
      }finally{
    	  if(r != null) try{  r.close() ; }catch(IOException ee ){/* to late to react */}
      }
   }
   private String [] _getRealTag( String tagName ){
      if( ( ! isDirectory() ) || ( ! isPnfs() ) )return null ;
      File f = new File( this , ".(tag)("+tagName+")" ) ;
      if( ! f.exists() )return null ;
      BufferedReader r = null ;
      try{
         List<String> v = new Vector<String>() ;
         String line = null ;
         r = new BufferedReader( new FileReader( f ) ,32) ;
         while( ( line = r.readLine() ) != null ){
            v.add( line ) ;
         }

         String [] a = new String[v.size()] ;

         return v.toArray(a) ;
      }catch( IOException ee ){
         return null ;
      }finally{
    	  if(r != null) try{  r.close() ; }catch(IOException ee ){/* to late to react */}
      }
   }
   public static String [] getPnfsMountpoints() throws FileNotFoundException {

      String [] mountpoints = null ;
      try{
          mountpoints = getPnfsMountpointsFrom( "/etc/mnttab" )  ;
      }catch( FileNotFoundException ee ){
          try{
              mountpoints = getPnfsMountpointsFrom( "/etc/mtab" ) ;
          }catch( FileNotFoundException ff ){
              throw new FileNotFoundException( "Not found mnttab,mtab" ) ;
          }
      }
      return mountpoints ;
   }
   private static String [] getPnfsMountpointsFrom( String mtab )
           throws FileNotFoundException {

      BufferedReader  in = null;
      String          line , mp , type ;
      StringTokenizer st ;
      List<String>          mountpoints = new Vector<String>() ;
      //
      //
      in = new BufferedReader(new FileReader( mtab ) );

      try{
         while(  ( line = in.readLine() ) != null    ){
            try{
               st = new StringTokenizer( line ) ;
               if(  st.countTokens() < 4 )continue ;
               st.nextToken() ;
               mp   = st.nextToken() ;
               type = st.nextToken() ;
            }catch(Exception ae ){
               continue ;
            }

            if( ! type.equals( "nfs" ) )continue ;
            FileInputStream pnfs = null ;
            File mpf = new File( mp ) ;
            try{
               pnfs = new FileInputStream( new File( mpf , ".(const)(x)" ) ) ;
            }catch( Exception ie3 ){
               continue ;
            }finally{
               if( pnfs != null) try{ pnfs.close() ; }catch(IOException iie ){}
            }
            mountpoints.add( mp );
         }
      }catch( IOException e ){
         return new String[0] ;
      }finally{
         try{ in.close()  ; }catch(IOException xioe ){}
      }
      String [] out = new String[mountpoints.size()] ;

      return  mountpoints.toArray(out);
    }
   /*
   public String getAbsolutePath(){
      System.out.println( "Absolute path required" ) ;
      return super.getAbsolutePath() ;
   }
   public String getCanonicalPath() throws IOException {
      System.out.println( "Canonical path required" ) ;
      return super.getCanonicalPath() ;
   }
   */
    public static PnfsFile getFileByPnfsId(String mountpoint, PnfsId id) throws CacheException {
       PnfsFile mp = new PnfsFile(mountpoint);
       if ((!mp.isDirectory()) || (!mp.isPnfs())) {
           throw new IllegalArgumentException("mountpoint [" + mountpoint + "] does not exist or not in the pnfs");
       }
       PnfsFile f = new PnfsFile(mountpoint, ".(access)(" + id + ")", id);
       if (!f.exists()) {
           boolean deleted = isDeleted(id);
           if (deleted)
               throw new FileNotFoundCacheException(id.toString());    // Old code
           else
               throw new CacheException(CacheException.NOT_IN_TRASH, "Not in trash: " + id.toString());
       }
       return f;
   }

    public static PnfsFile getFileByPnfsId(File mountpoint, PnfsId id) throws CacheException {
        PnfsFile mp = new PnfsFile(mountpoint.getAbsolutePath());
        if ((!mp.isDirectory()) || (!mp.isPnfs())) {
            throw new IllegalArgumentException("mountpoint [" + mountpoint + "] does not exist or not in the pnfs");
        }
        PnfsFile f = new PnfsFile(mountpoint.getAbsolutePath(), ".(access)(" + id + ")", id);
        if (!f.exists()) {
            boolean deleted = isDeleted(id);
            if (deleted)
                throw new FileNotFoundCacheException(id.toString());    // Old code
            else
                throw new CacheException(CacheException.NOT_IN_TRASH, "Not in trash: " + id.toString());
        }
        return f;
    }

    /**
     */
   protected static List<String> getServerAttributes( File mountpoint , String key )throws IOException {
      if( ( ! mountpoint.exists()      ) ||
          ( ! mountpoint.isDirectory() )    )
           throw new
           IllegalArgumentException( "Mountpoint doesn't exist or is not directory");

      File magicFile  = new File(mountpoint,".(config)("+key+")") ;
      BufferedReader  snbr = new BufferedReader(
                             new FileReader( magicFile ) ) ;

      List<String> list  = new ArrayList<String>() ;

      try {
            for (String line = null; (line = snbr.readLine()) != null;) {
                line = line.trim();
                /*
                 * skip empty lines and comments
                 */
                if (line.length() == 0) continue;
                if (line.charAt(0) == '#') continue;

                list.add(line);
            }
        } finally {
            try {
                snbr.close();
            } catch (IOException eeee) {
                // take is easy
            }
        }
      return list ;
   }
   protected static String getServerAttribute( File mountpoint , String key )throws IOException {
      List<String> list = getServerAttributes( mountpoint , key ) ;

      if( list.isEmpty()  ) {
          throw new
          IOException( "Couldn't find valid value for "+key ) ;
      }

      return list.get(0) ;
   }
   public static String getServerName( File mountpoint )throws IOException {
      return getServerAttribute( mountpoint , "serverName" ) ;
   }
   public static String getServerRoot( File mountpoint )throws IOException {
      return getServerAttribute( mountpoint , "serverRoot" ) ;
   }
   public static String getServerId( File mountpoint )throws IOException {
      return getServerAttribute( mountpoint , "serverId" ) ;
   }
   public static Map<PnfsId, String> getServerRoots( File mountpoint )throws IOException {
      List<String>     list    = getServerAttributes( mountpoint , "serverRoot" ) ;
      HashMap<PnfsId, String>  map     = new HashMap<PnfsId, String>();

      for ( String entry : list ){
          StringTokenizer st = new StringTokenizer(entry);
          try{
              PnfsId pnfsId = new PnfsId( st.nextToken() ) ;
              String value  =  st.hasMoreTokens() ? st.nextToken() : "." ;
              map.put( pnfsId , value ) ;
          }catch(Exception e){
              throw new IOException("Syntax error in serverRoot");
          }
      }
      return map ;
   }
   public static PnfsId getMountId( File mountpoint )throws IOException {
      Map<String, String> hash = getDirectoryCursor( mountpoint ) ;
      String entry = hash.get( "mountID" ) ;
      if( entry == null )
        throw new
        NoSuchElementException("Syntax Error in fileysstem cursor");

      return new PnfsId(entry);
   }
   public static Map<String, String> getDirectoryCursor( File directory ) throws IOException{
      if( ( ! directory.exists()      ) ||
          ( ! directory.isDirectory() )    )
           throw new
           IllegalArgumentException( "Argument doesn't exist or is not dir");
      File magicFile  = new File(directory,".(get)(cursor)") ;
      BufferedReader snbr = new BufferedReader(
                            new FileReader( magicFile ),128 ) ;
      Map<String, String> hash = new Hashtable<String, String>() ;
      try{
        while(true){
           String tmp = null , key = null , value = null ;
           try{
              if( ( tmp = snbr.readLine() ) == null )break ;
           }catch( EOFException ioe ){ break ; }
           StringTokenizer st = new StringTokenizer(tmp,"=") ;
           try{
              key = st.nextToken() ;
              value = st.nextToken() ;
              if( ( key.length() == 0 ) ||
                  ( value.length() == 0 )    )continue ;
              hash.put( key , value ) ;
           }catch(Exception eee ){
              continue ;
           }
        }
      }finally{
         try{ snbr.close() ; }catch(IOException eeee ) {}
      }
      return hash ;
   }
   public static File [] listBasePnfsRoots() {
      //
      // solaris ??
      //
      Hashtable<String, Object[]> hash = new Hashtable<String, Object[]>() ;
      File mtab = new File( "/etc/mnttab" ) ;
      if( ! mtab.exists() ){
         //
         // linux or irix
         //
         mtab = new File( "/etc/mtab" ) ;
         if( ! mtab.exists() )return new File[0]  ;
      }


      BufferedReader br;
      try {
          br = new BufferedReader( new FileReader( mtab ));
      } catch (FileNotFoundException e) {
          _logNameSpace.debug( mtab + ": " + e.getMessage());
          return new File[0] ;
      }

      /*
       *  /etc/mtab files have the following format:
       *
       *  <src> <mnt-point> <fs-type> <mnt-options> <dump-freq> <fsck-seq>
       *
       *  Tokens are space-separated.  <src> is either some hardware
       *  device or some token that makes sense to the filesystem.
       *  <mnt-point> is a directory within the VFS. <mnt-options> is a
       *  comma-separated list of keyword,value pairs that are joined by an
       *  '=' symbol.
       */
      try {
          String line;

          while( ( line = br.readLine() ) != null ){
              StringTokenizer st = new StringTokenizer(line);

              // Silently skip empty lines.
              if( !st.hasMoreTokens())
                  continue;

              st.nextToken() ; // ignore <src> token

              if( !st.hasMoreTokens()) {
                  _logNameSpace.debug( "Skipping line with only one token: " + line);
                  continue;
              }

              String fs = st.nextToken() ;
              _logNameSpace.debug( "Checking fs mounted at: " + fs);

              //
              //  get the server name
              //
              File   mountpoint = new File(fs) ;

              try {
                  String serverName = getServerName( mountpoint ) ;
                  //
                  // get the directory cursor ( to dist. io/fs )
                  //
                  Map<String, String> cursor  = getDirectoryCursor( mountpoint ) ;
                  String dirPerm    = cursor.get("dirPerm") ;
                  //
                  // only allow 'noio' mountpoints
                  //
                  if( ( dirPerm == null           ) ||
                      ( dirPerm.length() < 16     ) ||
                      ( dirPerm.charAt(14) != '0' ) ) {
                      _logNameSpace.debug( fs + " failed dirPerm check.");
                      continue ;
                  }

                  String mountId = cursor.get( "mountID" ) ;
                  if( mountId == null ) {
                      _logNameSpace.debug( fs + " failed mountID check");
                      continue ;
                  }
                  PnfsId id = new PnfsId( mountId ) ;
                  //
                  // look for the smallest mountpoint == largest filesystem
                  //
                  Object [] r = hash.get( serverName ) ;
                  if( r == null ){
                      _logNameSpace.debug( "Adding " + fs + " as initial candidate for " + mountId);
                     r = new Object[2] ;
                     r[0] = fs ;
                     r[1] = id ;
                     hash.put( serverName , r ) ;
                  }else{
                     PnfsId storedId = (PnfsId)r[1] ;
                     if(  id.compareTo( storedId ) < 0 ){
                       //
                       // found a smaller one
                       //
                       _logNameSpace.debug( "Updating candidate for " + mountId + " using " + fs);
                       r = new Object[2] ;
                       r[0] = fs ;
                       r[1] = id ;
                       hash.put( serverName , r ) ;
                     } else {
                         _logNameSpace.debug( "Ignoring candidate " + fs + " as higher mount-point already discovered");
                     }
                  }
               } catch (IOException e) {
                   _logNameSpace.debug( "  skipping entry " + fs + " as it triggered " + e.getClass().getSimpleName() + ": " + e.getMessage());
               } catch (IllegalArgumentException e) {
                   _logNameSpace.debug( "  skipping entry " + fs + " as it triggered " + e.getClass().getSimpleName() + ": " + e.getMessage());
               }
            }
      } catch (IOException e) {
          _logNameSpace.debug( "Skipping parsing of mount-points: " + e.getMessage());
      }finally{
          try {
              br.close();
          } catch(IOException ee) {
              _logNameSpace.info( "IOException whilst closing mtab file");
          }
      }

      File [] results = new File[hash.size()] ;
      Enumeration<Object []> e = hash.elements() ;
      for( int i = 0 ; e.hasMoreElements() ; i++ ){
         Object [] x = e.nextElement() ;
         results[i] = new File(x[0].toString());
      }
      return results ;
   }
   public static class VirtualMountPoint {
       //
       // per server
       //
       private File _basicMountPoint = null ;
       private String _serverId      = null ;
       private String _serverName    = null ;
       private PnfsId _mountId       = null ;
       //
       // per virtual mount point
       //
       private String _virtualGlobalPath = null ;
       private String _virtualLocalPath  = null ;
       private PnfsId _virtualMountId    = null ;
       private String _virtualPnfsPath   = null ;
       private VirtualMountPoint( File basicMountPoint ,
                                  String serverName ,
                                  String serverId ,
                                  PnfsId mountId    ){
           _basicMountPoint = basicMountPoint ;
           _serverId        = serverId ;
           _serverName      = serverName ;
           _mountId         = mountId ;
       }
       private void setVirtual( PnfsId mountId ,
                                String pnfsPath ,
                                String localPath ,
                                String globalPath ){
           _virtualMountId    = mountId ;
           _virtualPnfsPath   = pnfsPath ;
           _virtualLocalPath  = localPath ;
           _virtualGlobalPath = globalPath ;
       }
       public File getRealMountPoint(){ return _basicMountPoint ; }
       public PnfsId getRealMountId(){ return _mountId ;}
       public PnfsId getVirtualMountId(){ return _virtualMountId ; }
       public String getServerId(){ return _serverId ; }
       public String getServerName(){ return _serverName ; }
       public String getVirtualPnfsPath(){ return _virtualPnfsPath ; }
       public String getVirtualLocalPath(){ return _virtualLocalPath ; }
       public String getVirtualGlobalPath(){ return _virtualGlobalPath ; }
       @Override
    public String toString(){
          return _serverId+"@"+_serverName+":"+
                 _virtualLocalPath+" -> "+_virtualGlobalPath;}

   }
   public static List<VirtualMountPoint> getVirtualMountPoints( File mountPoint )throws IOException {
      List<VirtualMountPoint>   list       = new ArrayList<VirtualMountPoint>() ;
      PnfsId mountId    = getMountId(mountPoint)  ;
      String serverName = getServerName(mountPoint)  ;
      String serverId   = getServerId(mountPoint) ;
      Map<PnfsId, String>    map        = getServerRoots(mountPoint) ;
      String serverMountPath = pathfinder( mountPoint , mountId.toString() ) ;

      _logNameSpace.debug( "Building VirtualMountPoint for " + mountPoint);


      for(Map.Entry<PnfsId, String> entry: map.entrySet() ){

         PnfsId virtualMountId = entry.getKey() ;
         //
         // virtualMountpoint id is smaller than our real mountpoint
         //
         if( mountId.compareTo(virtualMountId) > 0 ) {
             _logNameSpace.debug( "  skipping entry as virtualMountId (" + virtualMountId + ") is smaller than our real mountpoint (" + mountId + ")");
             continue ;
         }

         String virtualPnfsPath = pathfinder( mountPoint , virtualMountId.toString()) ;
         String diff = virtualPnfsPath.substring(serverMountPath.length());

         VirtualMountPoint vmp =
             new VirtualMountPoint(mountPoint , serverName , serverId , mountId ) ;

         String virtualMountPath = entry.getValue() ;

         vmp.setVirtual( virtualMountId ,
                         virtualPnfsPath ,
                         mountPoint + diff ,
                         "/pnfs/"+
                           serverId+
                           (virtualMountPath.equals(".")?"":
                            virtualMountPath.equals("*")?diff:
                           "/"+virtualMountPath)  ) ;
         list.add( vmp ) ;
      }
      return list ;

   }
   public static List<VirtualMountPoint> getVirtualMountPoints()throws IOException {
      File [] mountpoints = listBasePnfsRoots() ;
      List<VirtualMountPoint>    list        = new ArrayList<VirtualMountPoint>() ;
      for(  int i = 0 ; i < mountpoints.length ; i++ ){
         try{
            list.addAll( getVirtualMountPoints( mountpoints[i] )  ) ;

         }catch(IOException ioe ){
            _logNameSpace.debug(  "IOException whilst building list of virtual mount-points: " + ioe.getMessage());
            throw ioe ;
         } catch( RuntimeException rte) {
            _logNameSpace.debug(  "Problem whilst building list of virtual mount-points" + rte.getClass().getSimpleName() + ":", rte);
         }catch(Exception gioe ){
            _logNameSpace.debug(  "Received " + gioe.getClass().getSimpleName()+ " : " + gioe.getMessage());
            // FIXME : what about run time exceptions , e.g. OutOfMemoryExceptions
         }
      }
      return list ;
   }
   public static void main( String [] args ){
      if( args.length < 1 ){
         System.out.println( "Usage : ... <command> <path> [arguments]" ) ;
         System.out.println( "Usage : ... ispnfs <path>" ) ;
         System.out.println( "Usage : ... id <path>" ) ;
         System.out.println( "Usage : ... gettags <dirPath>" ) ;
         System.out.println( "Usage : ... gettag <dirPath> <tagname>" ) ;
         System.out.println( "Usage : ... readlevel <dirPath> <level>" ) ;
         System.out.println( "Usage : ... readxlevel <mountpoint> <pnfsId> <level>" ) ;
         System.out.println( "Usage : ... setsize <filePath> <size>" ) ;
         System.out.println( "Usage : ... psetsize <mountpoint>  <pnfsId> <size>" ) ;
         System.out.println( "Usage : ... ptag <mountpoint>  <pnfsId> [<name>]" ) ;
         System.out.println( "Usage : ... mountpoints" ) ;
         System.out.println( "Usage : ... xmountpoints" ) ;
         System.out.println( "Usage : ... nameof <mountpoint> <pnfsId>" ) ;
         System.out.println( "Usage : ... parent <mountpoint> <pnfsId>" ) ;
         System.out.println( "Usage : ... length <mountpoint> <pnfsId>" ) ;
         System.out.println( "Usage : ... pathfinder <mountpoint> <pnfsId>" ) ;
         System.out.println( "Usage : ... analyse" ) ;
         System.exit(4) ;
      }
      try{
         String command = args[0] ;
         String path    = args.length > 1 ? args[1] : ""  ;
         PnfsFile f = new PnfsFile( path ) ;
         if( command.equals( "ispnfs" ) ){
             System.out.println( path+" : "+f.isPnfs() ) ;
         }else if( command.equals( "id" ) ){
            PnfsId id = f.getPnfsId() ;
            if( id == null ){
              System.out.println( "Can't determine pnfs id" ) ;
            }else{
              System.out.println( path+" : "+f.getPnfsId().toString() ) ;
            }
         }else if( command.equals( "nameof" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            System.out.println( PnfsFile._getNameOf( new File(args[1]) , args[2] ) ) ;
         }else if( command.equals( "length" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;

            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            System.out.println( x.toString()+" : "+x.length() ) ;
         }else if( command.equals( "parentof" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            System.out.println( PnfsFile._getParentId( new File(args[1]) , args[2] ) ) ;
         }else if( command.equals( "pathfinder" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            System.out.println( PnfsFile.pathfinder( new File(args[1]) , args[2] ) ) ;
         }else if( command.equals( "gettag" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            if( ! f.isDirectory() ){
               System.err.println( "Not a directory : "+path ) ;
               System.exit(45);
            }
            String [] tag = f.getTag( args[2] ) ;
            if( tag == null ){
              System.out.println( "Tag "+args[2]+" not found" ) ;
              System.exit(4);
            }else{
              for( int j = 0 ; j < tag.length ; j++ ){
                 System.out.println( args[1]+" : "+tag[j] ) ;
              }
            }

         }else if( command.equals( "gettags" ) ){
            String [] tags = f.getTags() ;
            if( tags == null ){
              System.out.println( "No tags found" ) ;
              System.exit(4);
            }else{
              for( int j = 0 ; j < tags.length ; j++ ){
                 System.out.println( tags[j] ) ;
              }
            }
         }else if( command.equals( "readlevel" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            int level = new Integer( args[2] ).intValue() ;
            File lf  = f.getLevelFile( level ) ;
            if( lf == null ){
              System.out.println( "Can't get levelfile" ) ;
              System.exit(4);
            }
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( lf ) ) ;
               String line = null ;
               while( ( line = r.readLine() ) != null ){
                  System.out.println( line ) ;
               }
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "ptag" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;

            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            if( args.length == 3 ){
               String [][] tags = x.getPTags() ;
               for( int i = 0 ;i < tags.length ; i++ )
                 System.out.println( tags[i][1]+" -> "+tags[i][0] ) ;
            }else{
               String [] tag = x.getTag( args[3] ) ;
               if( tag == null ){
                  System.err.println("tag not found : "+args[3] ) ;
                  System.exit(4);
               }
               for( int i = 0 ;i < tag.length ; i++ )
                 System.out.println( tag[i] ) ;

            }
         }else if( command.equals( "getfiletype" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;

            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFileType" ) ;
              System.exit(4);
            }
            String fileType = x.getPnfsFileType() ;
            if( fileType == null ){
               System.err.println("Can't determine filetype" ) ;
               System.exit(5);
            }
            System.out.println( fileType ) ;
         }else if( command.equals( "parent" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;

            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFileType" ) ;
              System.exit(4);
            }
            PnfsId parentId = x.getParentId() ;
            if( parentId == null ){
               System.err.println("Can't determine parentId" ) ;
               System.exit(5);
            }
            System.out.println( ""+parentId ) ;
         }else if( command.equals( "readxlevel" ) ){
            if( args.length < 4 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id = new PnfsId( args[2] ) ;
            int level = new Integer( args[3] ).intValue() ;


            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            File lf  = x.getLevelFile( level ) ;
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( lf ) ) ;
               String line = null ;
               while( ( line = r.readLine() ) != null ){
                  System.out.println( line ) ;
               }
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "test" ) ){
            try{
               BufferedReader r = new BufferedReader(
                                   new FileReader( f ) ) ;
               r.close() ;
            }catch( IOException e ){
               System.err.println( "Exception : "+e ) ;
            }
         }else if( command.equals( "psetsize" ) ){
            if( args.length < 4 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            PnfsId id   = new PnfsId( args[2] ) ;

            PnfsFile x  = PnfsFile.getFileByPnfsId( f.getAbsolutePath() , id ) ;
            if( x == null ){
              System.out.println( "Can't get PnfsFile.getFileByPnfsId" ) ;
              System.exit(4);
            }
            int size = new Integer( args[3] ).intValue() ;
            boolean y= x.setLength( size ) ;
            System.out.println( "Result : "+y ) ;
          }else if( command.equals( "setsize" ) ){
            if( args.length < 3 ){
               System.err.println( "Not enought arguments" ) ;
               System.exit(45);
            }
            int size = new Integer( args[2] ).intValue() ;
            boolean x = f.setLength( size ) ;
            System.out.println( "Result : "+x ) ;
         }else if( command.equals( "mountpoints" ) ){
            String [] mps = PnfsFile.getPnfsMountpoints() ;
            for( int i = 0 ; i < mps.length ; i++ ){
               System.out.println( ""+i+" -> "+mps[i] ) ;
            }
         }else if( command.equals( "xmountpoints" ) ){
            File [] mps = PnfsFile.listBasePnfsRoots() ;
            for( int i = 0 ; i < mps.length ; i++ ){
               System.out.println( ""+i+" -> "+mps[i]+
                                  " "+getServerName(mps[i])+
                                  " "+getServerRoot(mps[i]) ) ;
            }
         }else if( command.equals( "analyse" ) ){
            File [] mountpoints = listBasePnfsRoots() ;
            for(  int i = 0 ; i < mountpoints.length ; i++ ){
               File mp = mountpoints[i] ;
               System.out.println( "Mountpoint : "+mp ) ;

               System.out.print( "  Mount Id    : ") ;
               PnfsId mountId = null ;
               try{
                  mountId = getMountId(mp)  ;
                  System.out.println( mountId.toString() ) ;
               }catch(Exception ee){
                  System.out.println(ee.getMessage());
               }
               System.out.print( "  Server Name : ") ;
               try{
                  System.out.println( getServerName(mp) ) ;
               }catch(Exception ee){
                  System.out.println(ee.getMessage());
               }
               System.out.print( "  Server Id   : ") ;
               try{
                  System.out.println( getServerId(mp) ) ;
               }catch(Exception ee){
                  System.out.println(ee.getMessage());
               }
               System.out.print( "  Server Root : ") ;
               try{
                  System.out.println( getServerRoot(mp) ) ;
               }catch(Exception ee){
                  System.out.println(ee.getMessage());
               }
               System.out.print( "  Server Roots: ") ;
               Map<PnfsId, String> map = null ;
               try{
                  map = getServerRoots(mp) ;
                  System.out.println("");
               }catch(Exception ee){
                  System.out.println(ee.getMessage());
               }
               String serverMountPath = pathfinder( mp , mountId.toString() ) ;
               Iterator<Map.Entry<PnfsId, String>> entries = map.entrySet().iterator() ;
               while( entries.hasNext() ){
                  Map.Entry<PnfsId, String> entry = entries.next() ;
                  System.out.println("    "+entry.getKey()+"  "+entry.getValue() ) ;
                  PnfsId virtualMountId = entry.getKey() ;
                  if( mountId.compareTo(virtualMountId) > 0 ){
                     System.out.println( "    -> invalid (serverId<mountId)");
                     continue ;
                  }
                  String virtualMountPath = pathfinder( mp , virtualMountId.toString()) ;
                  String diff = virtualMountPath.substring(serverMountPath.length());
                  System.out.println( "      VirtualMountPath : "+virtualMountPath ) ;
                  System.out.println( "      VirtualBasicPath : "+serverMountPath ) ;
                  System.out.println( "      VirtualLocalPath : "+mp+diff ) ;
                  System.out.println( "      VirtualGlobalPath: /pnfs/"+
                               getServerId(mp)+
                               (entry.getValue().equals(".")?"":
                                entry.getValue().equals("*")?diff:
                                "/"+entry.getValue()));
                  System.out.println( "      Diff : "+diff ) ;

               }
            }
         }
      }catch( IOException e ){
         e.printStackTrace() ;
         System.exit(4) ;
      }catch (CacheException ex){
         ex.printStackTrace() ;
         System.exit(4) ;
      }
   }

}
