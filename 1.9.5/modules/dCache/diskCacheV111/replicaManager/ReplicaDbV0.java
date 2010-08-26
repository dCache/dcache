// $Id$

package diskCacheV111.replicaManager ;

import  diskCacheV111.util.* ;

import  java.util.* ;
import  java.io.* ;

public class ReplicaDbV0 implements ReplicaDb {
   private File _base = null ;
   public ReplicaDbV0( File baseDb ) throws IllegalArgumentException {
      _base = baseDb ;
      if( ! _base.isDirectory() )
         throw new
         IllegalArgumentException("Not a directory : "+_base) ;
   }
   public ReplicaDbV0( String baseDb ) throws IllegalArgumentException {
      _base = new File( baseDb ) ;
      if( ! _base.isDirectory() )
         throw new
         IllegalArgumentException("Not a directory : "+_base) ;
   }
   public synchronized void addPool( PnfsId pnfsId , String poolName ){
       Set set = getSet( pnfsId ) ;
       set = set == null ? new HashSet() : set ;
       set.add(poolName) ;
       putSet( pnfsId , set ) ;
   }
   public void removePool( PnfsId pnfsId , String poolName ){
       Set set = getSet( pnfsId ) ;
       if( set == null )return ;
       set.remove(poolName) ;
       if( set.size() == 0 )new File( _base , pnfsId.toString() ).delete() ;
       else putSet( pnfsId , set ) ;
   }
   public int countPools( PnfsId pnfsId ){
      Set s = getSet( pnfsId ) ;
      return s == null ? 0 : s.size() ;
   }
   public void clearPools( PnfsId pnfsId ){
      new File( _base , pnfsId.toString() ).delete() ;
   }
   public Iterator getMissing( ){ return new ArrayList().iterator() ; }
   public Iterator getDeficient( ){ return new ArrayList().iterator() ; }
   public Iterator getRedundant( ){ return new ArrayList().iterator() ; }
   public Iterator getPools( ){ return new ArrayList().iterator() ; }
   public Iterator pnfsIds( String pools ){ return new ArrayList().iterator() ; }

   public class PnfsIdIterator implements Iterator {
   
      private File           _file   = null ;
      private BufferedReader _input  = null ;
      private boolean        _done   = false ;
      private Object         _stored = null ;
      public PnfsIdIterator( File tmp ){
         _file = tmp ;
         try{
            _input = new BufferedReader( new FileReader( _file ) ) ;
         }catch(Exception ee ){
            _done = true ;
         }
      }
      private Object readNext(){
          if( _done ) return null ;
          try{
             Object line = _input.readLine() ;
             if( line == null ){
                try{ _input.close() ; _file.delete() ; }catch(Exception ee ){}
                _done = true ;
                return null ;
             }
             return line ;
          }catch(Exception ee ){
             _done = true ;
             try{ _input.close() ; _file.delete() ; }catch(Exception eee ){}
             return null ;
          }
      }
      public boolean hasNext(){
         if( _stored != null )return true ;
         _stored = readNext() ;
         return _stored != null ;
      }
      public Object next(){
         if( _stored != null ){
            Object x = _stored ;
            _stored = null ;
            return new PnfsId( x.toString() ) ;
         }
         Object n = readNext() ;
         if( n == null ){
            throw new
            NoSuchElementException("PnfsIdIterator");
         }
         return new PnfsId( n.toString() ) ;
      }
      public void remove(){
         throw new UnsupportedOperationException("No remove");
      }
   }
   public void clearAll(){
      _base.list(
           new FilenameFilter(){
               public boolean accept( File dir , String filename ){
                  if( ! filename.startsWith("00") )return false ;
                  new File( dir , filename ).delete() ;
                  return false ;
               }  
           }
      ) ;  
   }
   public Iterator pnfsIds(){ 
      try{
      
         File tmp = File.createTempFile( "DIR" , ".list" , _base ) ;
         tmp.deleteOnExit() ;
         final PrintWriter writer = new PrintWriter( new FileWriter( tmp ) ) ;
         try{
            _base.list(

               new FilenameFilter(){

                   public boolean accept( File dir, String filename ){
                       if( ! filename.startsWith("00") )return false ;
                       writer.println(filename);
                       return false ;
                   }
               }

            ) ;
         }finally{
            try{ writer.close() ; }catch(Exception eee ){}
         }
         return new PnfsIdIterator( tmp ) ;
      }catch(Exception ee ){
      
      }
      return new ArrayList().iterator() ; 
      
   }
   public Iterator getPools( PnfsId pnfsId ){
      Set s = getSet( pnfsId ) ;
      return s == null ? new ArrayList().iterator() : s.iterator() ;
   }
   private void putSet( PnfsId pnfsId , Set set ){
      try{
         File x = new File( _base , pnfsId.toString() ) ;
         ObjectOutputStream ois = new ObjectOutputStream( new FileOutputStream( x ) ) ;
         try{
            ois.writeObject( set ) ;
         }finally{
            try{ ois.close() ; }catch(Exception ee ){}
         }
      }catch(Exception ee ){
      }

   }
   private Set getSet( PnfsId pnfsId ){
      try{
         File x = new File( _base , pnfsId.toString() ) ;
         ObjectInputStream ois = new ObjectInputStream( new FileInputStream( x ) ) ;
         try{
            return (Set)ois.readObject() ;
         }finally{
            try{ ois.close() ; }catch(Exception ee ){}
         }
      }catch(Exception ee ){
         return null ;
      }
   }
   public static void main( String [] args )throws Exception {
      ReplicaDbV0 db = new ReplicaDbV0( new File(args[0]) ) ;
      for( Iterator i = db.pnfsIds() ; i.hasNext() ; ){
          System.out.println( i.next().toString());
      }
      PnfsId pnfsId = new PnfsId("1234") ;
      db.addPool( pnfsId , "pool1" ) ;
      db.addPool( pnfsId , "pool2" ) ;
      for( Iterator i = db.getPools( pnfsId ) ; i.hasNext() ; ){
        System.out.println(" pnfsid : "+pnfsId+ " pool "+i.next());
      }
      db.removePool( pnfsId , "pool2" ) ;
      for( Iterator i = db.getPools( pnfsId ) ; i.hasNext() ; ){
        System.out.println(" pnfsid : "+pnfsId+ " pool "+i.next());
      }
      System.exit(0);
   }
}
