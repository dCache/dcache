package dmg.util.cdb ;

public interface CdbLockable {
   public static final int  READ   =  1 ;
   public static final int  WRITE  =  2 ;
   public static final int  NON_BLOCKING = 4 ;
   public static final int  COMMIT =  8 ;
   public static final int  ABORT  =  16 ;
   public void open( int flags ) throws CdbLockException, InterruptedException ;
   public void close( int flags ) throws CdbLockException ;
}
