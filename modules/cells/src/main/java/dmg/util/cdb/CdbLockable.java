package dmg.util.cdb ;

public interface CdbLockable {
   int  READ   =  1 ;
   int  WRITE  =  2 ;
   int  NON_BLOCKING = 4 ;
   int  COMMIT =  8 ;
   int  ABORT  =  16 ;
   void open(int flags) throws CdbLockException, InterruptedException ;
   void close(int flags) throws CdbLockException ;
}
