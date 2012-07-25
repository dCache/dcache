package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.io.* ;
import  java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetupManager extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(SetupManager.class);

   private String      _cellName;
   private Args        _args;
   private CellNucleus _nucleus;

   private String _defaultClass;
   private String _configDirectory;

   private File   _config;

   //
   //   create dmg.cells.services.SetupManager setupManager
   //              "<configDirectory> [-defaultClass=<defaultClass>]"
   //
   public SetupManager( String cellName , String args ) throws Exception {

      super( cellName , args , false ) ;

      _cellName = cellName ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;

      try{

         if( _args.argc() == 0 ) {
             throw new
                     IllegalArgumentException("Config directory not specified");
         }

         _config = new File( _configDirectory = _args.argv(0) ) ;
         if( ! _config.isDirectory() ) {
             throw new
                     IllegalArgumentException("Config directory not found : " + _config);
         }

         _defaultClass = _args.getOpt("defaultClass") ;
         _defaultClass = _defaultClass == null ? "default" : _defaultClass ;

         _log.info("defaultClass set to '"+_defaultClass+"'");


      }catch(Exception ee ){
         start() ;
         kill() ;
         throw ee ;
      }
      useInterpreter( true ) ;
      start() ;

   }
   private void getSetup( SetupInfoMessage info ) throws Exception {
      String className = info.getSetupClass() ;
      String name      = info.getSetupName() ;
      if( name == null ) {
          throw new
                  IllegalArgumentException("No Setup name specified");
      }


      className = className == null ? "default" : className ;
      File classDir = new File( _config , className ) ;
      if( ! classDir.isDirectory() ) {
          throw new
                  NoSuchElementException("class:" + className);
      }

      File setupFile = new File(classDir,name);
      if( ! setupFile.exists() ){
         setupFile = new File(classDir,"default");
         if( ! setupFile.exists() ) {
             throw new
                     NoSuchElementException(classDir.getName() + ":" + name);
         }
      }
      BufferedReader br =
            new BufferedReader(
                  new FileReader( setupFile ) ) ;
      StringBuilder sb = new StringBuilder() ;
      try{
         String buffer ;
         while( ( buffer = br.readLine() ) != null ){
           sb.append(buffer).append("\n") ;
         }
      }finally{
         try{ br.close() ; }catch(Exception ee ){}
      }
      info.setPayload( sb.toString());
   }
   private void putSetup( SetupInfoMessage info ) throws IOException {
      String className = info.getSetupClass() ;
      String name      = info.getSetupName() ;
      Object payload   = info.getPayload() ;

      if( name == null ) {
          throw new
                  IllegalArgumentException("No Setup name specified");
      }
      if( payload == null ) {
          throw new
                  IllegalArgumentException("No payload");
      }


      className = className == null ? "default" : className ;

      File classDir = new File( _config , className ) ;
      if( ! classDir.isDirectory() ){
         classDir.mkdir() ;
         if( ! classDir.isDirectory() ) {
             throw new
                     IOException("can't create class:" + className);
         }
      }
      File setupFile = new File(classDir,name);
      PrintWriter pw = new PrintWriter(
                           new FileWriter( setupFile ) ) ;

      try{
         pw.print(payload.toString());
      }finally{
         try{ pw.close() ; }catch(Exception ee ){}
      }

   }
   public String ac_test_$_2( Args args )throws Exception {
      String className = args.argv(0) ;
      String name      = args.argv(1) ;
      SetupInfoMessage info =
            new SetupInfoMessage( name , className ) ;

      CellMessage reply = sendAndWait(
                  new CellMessage( new CellPath("setupManager") ,
                                   info ) ,
                  10000 ) ;

      if( reply == null ) {
          throw new
                  Exception("Request timed out");
      }

      info = (SetupInfoMessage)reply.getMessageObject() ;
      Object obj = info.getPayload() ;
      if( obj == null ) {
          throw new Exception("No payload");
      }
      if( obj instanceof Exception ) {
          throw (Exception) obj;
      }
      return obj.toString();
   }
   public String hh_ls_setup = "<class> <name>" ;
   public String ac_ls_setup_$_2( Args args ) throws Exception {
      String className = args.argv(0) ;
      String name      = args.argv(1) ;

      File   classDir  = new File( _config , className );
      if( ! classDir.isDirectory() ) {
          throw new
                  IllegalArgumentException("Class not found : " + className);
      }

      File setupFile = new File( classDir , name ) ;
      if( ! setupFile.exists() ) {
          throw new
                  IllegalArgumentException("Setup not found in class >" + className + "< : " + name);
      }

      StringBuilder sb = new StringBuilder() ;
      BufferedReader br =
            new BufferedReader(
                  new FileReader( setupFile ) ) ;
      try{
         String buffer ;
         while( ( buffer = br.readLine() ) != null ){
           sb.append(buffer).append("\n") ;
         }
      }finally{
         try{ br.close() ; }catch(Exception ee ){}
      }
      return sb.toString();
   }
   public String hh_ls_class = "[<className>]";
   public String ac_ls_class_$_0_1( Args args ){
      StringBuilder sb = new StringBuilder() ;
      if( args.argc() == 0 ){
         File [] fileList = _config.listFiles(
            new FileFilter(){
               @Override
               public boolean accept(File filepath ){
                  return filepath.isDirectory();
               }
            }
         );
         for( int i = 0 ; i < fileList.length ; i++ ){
            sb.append(fileList[i].getName()).append("\n");
         }
         return sb.toString();
      }else{

         String className = args.argv(0) ;
         File classDir = new File( _config , className );
         if( ! classDir.isDirectory() ) {
             throw new
                     IllegalArgumentException("Class not found : " + className);
         }

         File [] fileList = classDir.listFiles(
            new FileFilter(){
               @Override
               public boolean accept(File filepath ){
                  return filepath.isFile();
               }
            }
         );
         for( int i = 0 ; i < fileList.length ; i++ ){
            sb.append(fileList[i].getName()).append("\n");
         }
         return sb.toString();

      }

   }
   private void removeSetup( SetupInfoMessage info ){

   }
   @Override
   public void messageArrived( CellMessage message ){
      Object obj = message.getMessageObject() ;
      SetupInfoMessage info = null ;
      try{
         if( obj instanceof SetupInfoMessage ){
            info = (SetupInfoMessage)obj ;
            if( info.getAction() == null ){
               throw new
               IllegalArgumentException("Action not defined" ) ;
            }else if( info.getAction().equals("get") ){
               getSetup( info ) ;
            }else if( info.getAction().equals("put") ){
               putSetup( info ) ;
            }else if( info.getAction().equals("remove") ){
               removeSetup( info ) ;
            }else{
               throw new
               IllegalArgumentException(
                  "Action not defined : "+info.getAction()
                                       ) ;

            }
         }
      }catch( Exception ee ){
         info.setPayload(ee);
      }
      try{
         message.revertDirection() ;
         sendMessage(message);
      }catch(Exception ee ){
         _log.warn("Problems sending reply to "+message);
      }
   }

}
