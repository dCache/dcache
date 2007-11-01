package dmg.apps.libraryServer ;

import java.io.* ;
import java.util.* ;
import dmg.util.* ;


public class LSCommander extends CommandInterpreter {
   private LSProtocolHandler _handler = null ;
   private int               _section = -1 ;
   public String hh_open_server = "<filename> # opens in server mode" ;
   public String ac_open_server_$_1( Args args )throws Exception {
      if( _handler != null )
         throw new
         IllegalArgumentException( "File still Open" ) ;
      
      _handler = new LSProtocolHandler( args.argv(0) ,
                                        LSProtocolHandler.SERVER) ;
      
      return "" ;
   
   }
   public String hh_open_client = "<filename> # opens in client mode" ;
   public String ac_open_client_$_1( Args args )throws Exception {
      if( _handler != null )
         throw new
         IllegalArgumentException( "File still Open" ) ;
      
      _handler = new LSProtocolHandler( args.argv(0) ,
                                        LSProtocolHandler.CLIENT) ;
      
      return "" ;
   
   }
   public String hh_open_new = "<filename> <number of sections>" ;
   public String ac_open_new_$_2( Args args )throws Exception {
      if( _handler != null )
         throw new
         IllegalArgumentException( "File still Open" ) ;
      int sections = Integer.parseInt( args.argv(1) ) ;
      _handler = new LSProtocolHandler( args.argv(0) , sections ) ;
      
      return "" ;
   
   }
   public String ac_close( Args args )throws Exception {
      if( _handler == null )
         throw new
         IllegalArgumentException( "File Not Open" ) ;
      
      _handler.close() ;
      _handler = null ;
      return "" ;
   }
   private int getSection(Args args )throws Exception {
       String sec = args.getOpt("s") ;
       if( sec  != null )_section =  Integer.parseInt(sec) ;
       if( _section < 0 )
         throw new
         IllegalArgumentException( "Section not defined" ) ;
       return _section; 
   }
   public String hh_get = "[-s=sectionNum]" ;
   public String ac_get( Args args )throws Exception {
      if( _handler == null )
        throw new 
        IllegalArgumentException("File not open" ) ;
      int section = getSection(args) ;
      _handler.getSemaphore( section ) ;
      return "Got Semaphore for section "+_handler.getSectionName(section)+"\n" ;
   }
   public String hh_release = "[-s=sectionNum]" ;
   public String ac_release( Args args )throws Exception {
      if( _handler == null )
        throw new 
        IllegalArgumentException("File not open" ) ;
      int section = getSection(args) ;
      _handler.releaseSemaphore( section ) ;
      return "Released Semaphore for section "+_handler.getSectionName(section)+"\n" ;
   }
   public String hh_set_command = " [-s=sectionNum] \"<command string>\"" ;
   public String ac_set_command_$_1( Args args )throws Exception {
      if( _handler == null )
        throw new 
        IllegalArgumentException("File not open" ) ;
      int section = getSection(args) ;
      _handler.setCommandString( section , args.argv(0) ) ;
      return "" ;
   }
   public String hh_set_name = " [-s=sectionNum] \"<section name>\"" ;
   public String ac_set_name_$_1( Args args )throws Exception {
      if( _handler == null )
        throw new 
        IllegalArgumentException("File not open" ) ;
      int section = getSection(args) ;
      _handler.setSectionName( section , args.argv(0) ) ;
      return "" ;
   }
   public String ac_sections( Args args )throws Exception {
      if( _handler == null )
      throw new 
      IllegalArgumentException("File not open" ) ;
      
      int count = _handler.getSectionCount() ;
      StringBuffer sb = new StringBuffer() ;
      for( int i = 0 ; i < count ; i++ ){
         sb.append( _handler.getSectionName(i)+
                    "  ("+_handler.readSemaphore(i)+")  "+
                    _handler.getCommandString(i)+"\n") ;
      }
      return sb.toString() ;
   }
   public String hh_section = "[<newSection>}" ;
   public String ac_section_$_0_1( Args args )throws Exception {
      if( _handler == null )
      throw new 
      IllegalArgumentException("File not open" ) ;
      
      if( args.argc() > 0 ){
         int section = Integer.parseInt( args.argv(0) ) ;
         int count   = _handler.getSectionCount() ;
         if( ( section < 0 ) || ( section >= count ) )
           throw new
           IllegalArgumentException("Section out of range 0 <= "+section+" < "+count ) ;
         _section = section ;
      }
      if( _section < 0 )
         throw new
         IllegalArgumentException( "Section not defined" ) ;
      String sectionName = _handler.getSectionName(_section) ;
      return "Current section is : "+sectionName+"("+_section+")\n" ;
   }
   public String ac_exit( Args args )throws Exception {
      if( _handler != null )_handler.close() ;
      throw new CommandExitException("Done" ) ;
   }
   public static void main( String [] args )throws Exception {
      String line = null ;
      BufferedReader br = new BufferedReader( new InputStreamReader( System.in ) ) ;
      LSCommander commander = new LSCommander() ;
      System.out.print(" > ");
      try{
         while( ( line = br.readLine() ) != null ){
             System.out.print(commander.command(line));
             System.out.print(" > ");
         } 
      }catch(CommandExitException ee ){
      
      }
      System.out.println("");
      System.exit(0);       
   }

}
