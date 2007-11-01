package dmg.cells.nucleus ;

class CellThreadGroup extends ThreadGroup {
   private CellNucleus _nucleus ;
   CellThreadGroup( CellNucleus nucleus , ThreadGroup group , String name ){
      super( group , name ) ;
      _nucleus = nucleus ;
   }
   public void uncaughtException( Thread thread , Throwable t ){
      _nucleus.esay( "Thread : "+thread.getName()+ " got : "+t ) ;
      _nucleus.esay( t ) ;
   }
//   protected void finalize() throws Throwable {
//       _nucleus.say( "CellThreadGroup finalize : "+_nucleus.getCellName());
//   }
}
