package diskCacheV111.vehicles;

import java.util.Map ;

/**
  *  Implementations of this thing travel from the particular
  *  Door to the corresponding mover. The relevant information
  *  is extracted out of Pnfs with the help of the
  *  responsible implementation of <strong>StorageInfoExtractor</strong>.
  */
public interface StorageInfo extends java.io.Serializable {
    static final long serialVersionUID = 1623022255585848311L;
    /**
      *   The storage class is a unique string, identifying
      *   this particular storage entity in a HSM independent
      *   way ( whatelse, its a string).
      *   The storage class reflects a HSM specific organizational
      *   unit. ( storage group for OSM, and file family for enstore).
      *   The storage class determines the grouping behaviour
      *   concerning <strong>deferredWrites</strong> and
      *   <strong>poolSelection</strong>.
      *   
      */
    public String getStorageClass() ;
    public void   setBitfileId( String bitfileId ) ;
    public String getBitfileId() ;
    /**
      * The 'cacheClass' can be used as alternative to chose the
      * appropriate 'pool'. Pnfs may provide the information
      * in the 'cacheClass' tag. May return 'null'  if not 
      * precified.
      */
    public String getCacheClass() ;
    /**
      * Returns the name of the HSM or the HSMInstance.
      */
    public String getHsm() ;
    /**
      * Get size of BitFile.
      */
    public long   getFileSize() ;
    /**
      *  Set size of BitFile
      */
    public void   setFileSize( long fileSize ) ;
    /**
      * Determines whether the file exists somewhere (cache or HSM)
      * or not. Currently isCreatedOnly returns true is the 
      * size of the level-0 file is not zero.
      */
    public boolean isCreatedOnly() ;
    /**
      * Determines whether there exists a copy of the file in the HSM
      * or not. Currently isCreatedOnly returns true if the
      * size of the level-1 file is not zero.
      */
    public boolean isStored() ;
    /**
      *  The storage info may contain HSM specific key value pairs.
      *  Nobody should rely on the existence of a particular key.
      *  A 'null' is returned if no related key-value pair could be
      *  found.
      */
    public String  getKey( String key ) ;
    public void    setKey( String key , String value ) ;
    /**
      * Returns a COPY of the internal key,value map.
      */
    public Map     getMap() ;
} 
