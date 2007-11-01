package dmg.apps.libraryServer ;

import java.io.* ;
import java.util.* ;

public class LSProtocolHandler {
   private RandomAccessFile _file = null ;
   private int              _mode = SERVER ;
   public  static final int  SERVER = -1 ;
   public  static final int  CLIENT = -2;
   private static final int  SECTION_SIZE = 512 ;
   private static final int  SECTION_NAME_SIZE = 32 ;
   private Object _fileLock = new Object() ;
   
   public class Section {
      private int _section = -1 ;
      private String _name   = "" ;
      private Section( int section ) throws IOException {
         _section = section ;
         _name    = getSectionName(section);
      }
      public int getSectionId(){ return _section ; }
      public String getName(){ return _name ; }
      public void getSemaphore()throws IOException, InterruptedException{
         LSProtocolHandler.this.getSemaphore(_section) ;
      }
      public void releaseSemaphore() throws IOException {
         LSProtocolHandler.this.releaseSemaphore(_section);
      }
      public String getCommandString() throws IOException {
         return LSProtocolHandler.this.getCommandString(_section) ;
      }
      public void setCommandString(String command)throws IOException {
         LSProtocolHandler.this.setCommandString(_section,command);
      }
      public char readSemaphore()throws IOException {
         return LSProtocolHandler.this.readSemaphore(_section);
      }
   
   }
   public LSProtocolHandler( String filename , int mode )throws IOException {
      if( mode < 0 ){
          _mode = mode ;
          File f = new File( filename ) ;
          if( ! f.exists() )
             throw new
             FileNotFoundException( "Not found : "+filename ) ;
             
          _file = new RandomAccessFile( filename , "rw" ) ;
          try{
             int len = (int)_file.length() ;
             if( ( len == 0 ) || ( ( len % SECTION_SIZE ) != 0 ) )
                throw new
                IOException( 
                "Not a valid LSP file (must be multiple of "+SECTION_SIZE+")"); 
          }catch(IOException ioe ){
             try{ _file.close() ; }catch(Exception ee){}
             throw ioe ;
          }
       }else{
          _mode = CLIENT ;
          File f = new File( filename ) ;
          if( f.exists() )
             throw new
             IOException( "File already exists: "+filename ) ;
          _file = new RandomAccessFile( filename , "rw" ) ;
          try{
             synchronized( _fileLock ){
                byte [] data = new byte[SECTION_SIZE] ;
                for( int i = 0 ; i < data.length ; i++ )data[i] = ' ' ;
                _file.seek(0);
                for( int i = 0 ; i < mode ; i++ ){
                   _file.write(data) ;
                } 
                for( int i = 0 ; i < mode ; i++ ){
                   setSectionName( i , "UNKNOWN-"+i) ;
                   setSemaphore(i,'c');
                } 

             }
          }catch(IOException ioe ){
             try{ _file.close() ; }catch(Exception ee){}
             throw ioe ;
          }
       }    
   }
   public Section getSection( int sectionId )throws IOException {
      return new Section(sectionId);
   }
   public int getSectionCount() throws IOException{
      return (int)_file.length() / SECTION_SIZE ;
   }
   public void getSemaphore( int sectionId )
      throws InterruptedException,
             IOException {
      waitFor( sectionId , _mode == SERVER ? 's' : 'c' ) ;
   }
   private void setSemaphore( int sectionId , char c )throws IOException {
      synchronized( _fileLock ){
         seek( sectionId , SECTION_NAME_SIZE) ;
         int g = (byte)c ;
         _file.writeByte(g) ;
      }
   }
   public void releaseSemaphore( int sectionId ) throws IOException{
      setSemaphore( sectionId , _mode == SERVER ? 'c' : 's' ) ;
   }
   private void seek( int sectionId )throws IOException {
      seek( sectionId , 0 ) ;
   }
   private void seek( int sectionId , int offset )throws IOException {
      if( _file.length() < ( ( sectionId + 1 ) * SECTION_SIZE ) )
         throw new
         IOException( "Illegal sectionId : area to small" ) ;
      _file.seek( (long)(sectionId * SECTION_SIZE + offset )) ;
   }
   private void waitFor( int sectionId , char c )
      throws InterruptedException,
             IOException {
      
      synchronized( _fileLock ){
         while( true ){
            seek( sectionId , SECTION_NAME_SIZE ) ;
            int g = _file.readByte() ;
            if( g == c )break ;        
            _fileLock.wait(5000) ;
         }
      
      }         
   }
   public synchronized char readSemaphore( int sectionId  )
      throws IOException {
             
      synchronized( _fileLock ){
            seek( sectionId , SECTION_NAME_SIZE ) ;
            return (char)_file.readByte() ;
      }
      
              
   }
   public String getCommandString( int sectionId ) throws IOException{
      byte [] data = new byte[SECTION_SIZE] ;
      synchronized( _fileLock ){
         seek(sectionId) ;
         int r = _file.read(data) ;
         if( r < data.length )
            throw new
            IOException( "reading beyond end of data : "+sectionId ) ;
      }
      int i= 0 ;
      for( i = SECTION_NAME_SIZE + 1 ; 
           ( i < data.length ) && 
           ( data[i] != 0    ) && 
           ( data[i] != '\n' ) ; i++ ) ;
      
      return (new String( data , SECTION_NAME_SIZE+1 , i-SECTION_NAME_SIZE-1 )).trim() ;
   }
   public String getSectionName( int sectionId ) throws IOException{
      byte [] data = new byte[SECTION_NAME_SIZE] ;
      synchronized( _fileLock ){
         seek(sectionId) ;
         int r = _file.read(data) ;
         if( r < data.length )
            throw new
            IOException( "reading beyond end of data : "+sectionId ) ;
      }
      int i= 0 ;
      for( i = 0 ; 
           ( i < data.length ) && 
           ( data[i] != 0    ) && 
           ( data[i] != '\n' ) ; i++ ) ;
      
      return (new String( data , 0 , i )).trim() ;
   }
   public void setSectionName( int sectionId , String sectionName )
          throws IOException{
   
      byte [] data = new byte[SECTION_NAME_SIZE] ;
      byte [] name = sectionName.getBytes() ;
      int i = 0 ;
      for( ; ( i < SECTION_NAME_SIZE ) &&
             ( i < name.length      )    ; i++ )data[i] = name[i] ;
      for( ; i < SECTION_NAME_SIZE ; i++ )data[i] = (byte)' ' ; 
      synchronized( _fileLock ){
         seek(sectionId) ;
         _file.write(data) ;
      }
   }
   public void setCommandString( int sectionId , String command )throws IOException{
      byte [] data = command.getBytes() ;
      if( data.length >= ( SECTION_SIZE - SECTION_NAME_SIZE - 2 ) )
         throw new
         IllegalArgumentException( "Command to long : "+data.length ) ;
         
      synchronized( _fileLock ){  
          seek(sectionId,SECTION_NAME_SIZE+1) ;
          _file.writeByte(' ') ;
          _file.write(data) ; 
          data = new byte[SECTION_SIZE - SECTION_NAME_SIZE - 2 - data.length] ;
          for( int i = 0 ; i < data.length ;i++ )data[i] = ' ';
          _file.write(data) ;
      } 
      return  ;
      
      
   }
   public void close() throws IOException{ 
       _file.close() ;
   }
   public static void main( String [] args )throws Exception {
   
       if( args.length < 1 ){
           System.err.println("Usage : ... <mapFilename>" ) ;
           System.exit(4);
        }
        
   }
         
}
