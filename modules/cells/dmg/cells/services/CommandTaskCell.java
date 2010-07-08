package dmg.cells.services ;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;
import  java.lang.reflect.*;
import  java.io.* ;
import  java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Patrick Fuhrmann patrick.fuhrmann@desy.de
 * @version 0.9, Aug 7, 2006
 *
 */

public class CommandTaskCell extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(CommandTaskCell.class);

   private String        _cellName = null ;
   private Args          _args     = null ;
   private CellNucleus   _nucleus  = null ;
   private ClientHandler _clientHandler = new ClientHandler() ;
   private HashMap       _cores   = new HashMap() ;
   private HashMap       _modules = new HashMap() ;

   public class CellCommandTaskCore  extends dmg.util.CommandInterpreter {
      private CellAdapter    _cell      = null ;
      private String         _name      = null ;
      private Args           _classArgs = null ;
      private Args           _taskArgs  = null ;
      private ModuleInfo    _moduleInfo = null ;
      private CellCommandTaskable _task = null ;

      private CellCommandTaskCore( String name , ModuleInfo moduleInfo , Args args ){
         _name       = name ;
         _moduleInfo = moduleInfo ;
         _classArgs  = moduleInfo._args ;
         _taskArgs   = args ;
      }
      public void setCellCommandTaskable( CellCommandTaskable task ){
         addCommandListener(task);
         _task = task ;
      }
      public Args getModuleArgs(){ return _classArgs ; }
      public Args getTaskArgs(){ return _taskArgs ; }
      public CellCommandTaskable getTaskable(){ return _task ; }
      public String getName(){ return _name ; }
      public String toString(){
          return super.toString() ;
      }
      public CellAdapter getParentCell(){ return CommandTaskCell.this ; }
      public void sendMessage( CellMessage message )throws Exception {
          CommandTaskCell.this.sendMessage( message , true , true , _task , 999999999L) ;
      }
      public void sendMessage( CellMessage message , long timeout )throws Exception {
          CommandTaskCell.this.sendMessage( message , true , true , _task , timeout) ;
      }
      public String getModuleName(){
         return _moduleInfo.getName() ;
      }
   }
   public interface CellCommandTaskable extends CellMessageAnswerable {
       public void timer() ;
       public void getInfo( PrintWriter pw ) ;
   }
   private class ModuleInfo {
      private Constructor _constructor = null  ;
      private Args        _args  = null ;
      private String      _name  = null ;
      private ModuleInfo( String name , Constructor constructor , Args args ){
         _constructor = constructor ;
         _args = args ;
         _name = name ;
      }
      public String getName(){return _name ; }
      public String toString(){
         return _name+" "+_constructor.getName();
      }
   }
   private class ClientInfo {

      private long   _time      = System.currentTimeMillis() ;
      private String _clientKey = null ;
      private CellCommandTaskCore _session   = null ;

      private ClientInfo( String key ){
          _clientKey = key ;
      }
      public boolean isAttached(){
         return _session != null ;
      }
      public CellCommandTaskCore getCore(){
         return _session ;
      }
      public void setCore( CellCommandTaskCore core ){
         _session = core ;
      }
      public String toString(){
         return _clientKey+" = "+( _session == null ? "not attached" : _session.toString() ) ;
      }
      public String getClientKey(){
         return _clientKey ;
      }
      public void touch(){
         _time = System.currentTimeMillis() ;
      }
      public CellCommandTaskCore detach(){
         CellCommandTaskCore session = _session ;
         _session = null ;
         return session ;
      }
   }
   private class ClientHandler {

      private HashMap _clientHash      = new HashMap() ;
      private long    _maxSessionLogin = 10L * 60L * 1000L ;

      public Collection clients(){
         return _clientHash.values() ;
      }
      public ClientInfo getThisClient(){
         String     key  = getClientKey() ;
         ClientInfo info = (ClientInfo)_clientHash.get(key) ;
         if( info == null )_clientHash.put( key , info = new ClientInfo(key) ) ;
         info.touch();
         return info ;
      }
      public CellCommandTaskCore detach(){
         return getThisClient().detach();
      }
      public CellCommandTaskCore detach( String clientKey ){
         ClientInfo info = (ClientInfo)_clientHash.get(clientKey);
         if( info == null )return null ;
         return info.detach();
      }
      public void setLogoutTimer( long interval ){
         _maxSessionLogin = interval ;
      }
      public long getLogoutTimer(){ return _maxSessionLogin ; }
      public void cleanUp(){
         ArrayList list = new ArrayList( clients() ) ;
         long now = System.currentTimeMillis() ;
         for( Iterator i = list.iterator() ; i.hasNext() ; ){
             ClientInfo info = (ClientInfo)i.next() ;
             if( ( now - info._time ) > _maxSessionLogin ){
                String key = info.getClientKey() ;
                _log.info("Timer : "+key+" idle time exceeded" ) ;
                _clientHash.remove(key);
             }
         }
      }
   }
   private class Scheduler implements Runnable {
       private long   _sleepInterval = 60L * 1000L ;
       private Thread _worker        = null ;
       private Scheduler() throws Exception {
          (_worker = _nucleus.newThread(this,"Scheduler") ).start() ;
       }
       public void run(){
          _log.info("Scheduler worker started");
          while( ! Thread.interrupted() ){
             try{
                 Thread.sleep(_sleepInterval);
             }catch(InterruptedException ee ){
                 _log.info("Worker Thread interrupted");
                 break ;
             }
             try{
                doTiming();
             }catch(Throwable t ){
                 _log.warn("Problem in 'doTiming' : "+t, t);
             }
          }
       }
   }
   public CommandTaskCell( String cellName , String args ) throws Exception {
      super( cellName , args , false ) ;
      _cellName = cellName ;
      _args     = getArgs() ;
      _nucleus  = getNucleus() ;
      useInterpreter( true ) ;

      try{
         new Scheduler();
      }catch(Exception ee ){
         start() ;
         kill() ;
         throw ee;
      }
      start() ;
   }
   private void doTiming(){
      _nucleus.updateWaitQueue();
      ArrayList list = new ArrayList( _cores.values() ) ;
      for( Iterator i = list.iterator() ; i.hasNext() ; ){
         CellCommandTaskCore core = (CellCommandTaskCore)i.next() ;
         try{
            core._task.timer() ;
         }catch(Throwable t){
             _log.warn("Throwable in task : "+core.getName()+" : "+t, t);
         }
      }
      _clientHandler.cleanUp();

   }
   private String getClientKey(){
      CellMessage commandMessage = getThisMessage() ;
      CellPath source = commandMessage.getSourcePath() ;
      return ""+source.getCellName()+"@"+source.getCellDomainName() ;
   }
   //
   public String hh_set_logout_time = "<timeInSeconds>";
   public String ac_set_logout_time_$_1( Args args ){
       long interval = Long.parseLong(args.argv(0)) * 1000L ;
       if( interval <= 0 )
         throw new
         IllegalArgumentException("Time must be > 0");

       _clientHandler.setLogoutTimer(interval);
       return "" ;
   }
   public String hh_ls_task = "[-l]" ;
   public String ac_ls_task( Args args ){
      ClientInfo client =_clientHandler.getThisClient() ;
      boolean  extended = args.getOpt("l") != null ;
      StringBuffer sb = new StringBuffer() ;
      for( Iterator i = _cores.values().iterator() ; i.hasNext() ; ){
         CellCommandTaskCore core = (CellCommandTaskCore)i.next() ;
         sb.append( core.getName() ) ;
         if( extended )
            sb.append(" ").
               append(core._moduleInfo.getName()).
               append(" {").append(core._task.toString()).append("}");
         sb.append("\n");
      }
      return sb.toString() ;
   }
   public String hh_ls_module = "" ;
   public String ac_ls_module( Args args ){
      ClientInfo client = _clientHandler.getThisClient() ;

      StringBuffer sb = new StringBuffer() ;
      for( Iterator i = _modules.entrySet().iterator() ; i.hasNext() ; ){
         Map.Entry entry = (Map.Entry)i.next() ;
         sb.append( entry.getKey().toString() ).
            append( " -> ").
            append( ((ModuleInfo)entry.getValue())._constructor.getName()).
            append("\n");
      }
      return sb.toString() ;
   }
   public String hh_ls_client = "" ;
   public String ac_ls_client( Args args ){

      ClientInfo client =_clientHandler.getThisClient() ;
      StringBuffer sb = new StringBuffer() ;
      String ourClientKey = client.getClientKey() ;
      for( Iterator i = _clientHandler.clients().iterator() ; i.hasNext() ; ){
         ClientInfo info = (ClientInfo)i.next() ;
         String clientKey = info.getClientKey() ;
         sb.append( info.getClientKey() ).
            append(" ").append( ourClientKey.equals(clientKey) ? "*" : " " ).
            append(" [").
            append( ( System.currentTimeMillis()-info._time) / 1000L ).append("] -> ");
         if( ! info.isAttached() )
            sb.append("not attached\n") ;
         else
            sb.append(info.getCore().getName()).append("\n");
      }
      return sb.toString() ;

   }
   public String hh_create_task = "<taskName> <moduleName>";
   public String ac_create_task_$_2( Args args ) throws Throwable {


      String taskName   = args.argv(0) ;
      String moduleName = args.argv(1) ;

      try{

          ClientInfo client = _clientHandler.getThisClient() ;
          if( client.isAttached() )
            throw new
            IllegalArgumentException("Already attached to "+client.getCore().getName() ) ;

           CellCommandTaskCore core = (CellCommandTaskCore)_cores.get(taskName);
           if( core != null )
             throw new
             IllegalArgumentException("Task already exists : "+taskName);

           ModuleInfo moduleInfo = (ModuleInfo)_modules.get( moduleName ) ;
           if( moduleInfo == null )
              throw new
              NoSuchElementException("Module not found : "+moduleName);

           core = new CellCommandTaskCore( taskName , moduleInfo , args ) ;

           Constructor cons = moduleInfo._constructor ;

           Object obj = cons.newInstance( new Object [] { core } ) ;
           if( ! ( obj instanceof CellCommandTaskable ) )
              throw new
              Exception("PANIC : module doesn't interface CellCommandTaskable");

           core.setCellCommandTaskable( (CellCommandTaskable) obj );
           //
           // add instance
           //
           _cores.put( taskName , core ) ;
           //
           // attach
           //
           client.setCore( core ) ;

           return "Task <"+taskName+"> created and attached to (us) ["+client.getClientKey()+"]" ;

       }catch(InvocationTargetException ite ){
           Throwable cause = ite.getCause() ;
           _log.warn("Problem creating "+moduleName+" InvocationTargetException cause : "+cause, cause);
           if( cause != null ){
              throw cause ;
           }else{
              throw ite ;
           }

       }catch(Exception ee ){
           _log.warn("Problem creating "+moduleName+" "+ee, ee);
           throw ee ;
       }
   }
   public String hh_attach = "<sessionName>" ;
   public String ac_attach_$_1( Args args ){

      ClientInfo client = _clientHandler.getThisClient() ;
      if( client.isAttached() )
        throw new
        IllegalArgumentException("Already attached to "+client.getCore().getName() ) ;

      String taskName = args.argv(0);
      CellCommandTaskCore core = (CellCommandTaskCore)_cores.get(taskName) ;
      if( core == null )
          throw new
          NoSuchElementException("Task not found : "+taskName);

      client.setCore(core);

      return  "Task <"+taskName+"> attached to (us) ["+client.getClientKey()+"]" ;
   }
   public String hh_detach = "[<clientKey>]" ;
   public String ac_detach_$_0_1( Args args ){

      if( args.argc() == 0 ){
         CellCommandTaskCore core = _clientHandler.detach() ;
         if( core == null )return "Wasn't attached";
         return "Detached from : "+core.getName() ;
      }else{
         String clientKey = args.argv(0);
         CellCommandTaskCore core = _clientHandler.detach( clientKey ) ;
         if( core == null )return "Wasn't attached";
         return "Detached from : "+core.getName() ;
      }


   }
   public String hh_do = "<module specific commands>" ;
   public Object ac_do_$_1_999( Args args ) throws Exception {
      return executeLocalCommand( args ) ;
   }
   public String hh_task = "<module specific commands>" ;
   public Object ac_task_$_1_999( Args args ) throws Exception {
      return executeLocalCommand( args ) ;
   }
   public Object command( Args args )throws CommandException {
      Args copyArgs = (Args)args.clone() ;
      try{
          return super.command(args) ;
      }catch(CommandSyntaxException ee ){
          //_log.warn("!!1 first shot failed : "+ee);
          return executeLocalCommand( copyArgs ) ;
      }
   }
   private Object executeLocalCommand( Args args ) throws CommandException {

      ClientInfo client = _clientHandler.getThisClient() ;
      if( ! client.isAttached() )
        throw new
        IllegalArgumentException("Not attached") ;

      CellCommandTaskCore core = client.getCore() ;

      Object obj = core.command( args ) ;
      if( obj == null )
        throw new
        CommandException("Command returned null");

      return obj ;

   }
   public void getInfo( PrintWriter pw ){

      ClientInfo client = _clientHandler.getThisClient() ;

      pw.println("      Logout Time : "+(_clientHandler.getLogoutTimer()/1000L) + " seconds") ;
      pw.println("  Number of Tasks : "+_cores.size() ) ;
      pw.println("Number of Clients : "+_clientHandler.clients().size() ) ;
      pw.println("    Our Client Id : "+_clientHandler.getThisClient().getClientKey());
      CellCommandTaskCore core = client.getCore() ;
      pw.println("         Attached : "+( core == null ? "false" : core.getName() ) );
      if( core != null ){
         core._task.getInfo(pw);
      }
   }
   private Class [] _classSignature = {
       dmg.cells.services.CommandTaskCell.CellCommandTaskCore.class
   } ;
   public String hh_define_module = "<moduleName> <moduleClass>" ;
   public String ac_define_module_$_2( Args args )throws Exception {

       String moduleName = args.argv(0);
       String moduleClass = args.argv(1) ;

       Class       mc  = Class.forName( moduleClass ) ;
       Constructor mcc = mc.getConstructor( _classSignature ) ;

       _modules.put( moduleName , new ModuleInfo( moduleName , mcc , args ) ) ;

       return "" ;
   }
   public String hh_undefine_module = "<moduleName>" ;
   public String ac_undefine_module_$_1( Args args ){
       _modules.remove(args.argv(0));
       return "";
   }
   public void cleanUp(){

   }
   public void messageArrived( CellMessage msg ){

   }
   /**
     * EXAMPLE
     */
   static public class ModuleExample implements CommandTaskCell.CellCommandTaskable {

      private CellAdapter _cell = null ;
      private CommandTaskCell.CellCommandTaskCore _core = null ;

      public ModuleExample( CommandTaskCell.CellCommandTaskCore core ){
          _cell = core.getParentCell() ;
          _core = core ;
          _log.info("Started : "+core.getName());
      }
      public String hh_send = "<destination> <message>" ;
      public String ac_send_$_2( Args args ) throws Exception {
          CellMessage msg = new CellMessage( new CellPath( args.argv(0) ) , args.argv(1) ) ;
          _core.sendMessage(msg);
          return "" ;
      }
      public String hh_test = "<whatever>" ;
      public String ac_test_$_0_99( Args args ){
          StringBuffer sb = new StringBuffer() ;
          sb.append(" Module Args : ").append( _core.getModuleArgs().toString() ).append("\n");
          sb.append("   Task Args : ").append( _core.getTaskArgs().toString() ).append("\n");
          sb.append("Command Args : ").append( args.toString()).append("\n");
          return sb.toString() ;
      }
      public void answerArrived( CellMessage request , CellMessage answer ){
         _log.info("Answer arrived for task : "+_core.getName()+" : "+answer.getMessageObject().toString());
      }
      public void exceptionArrived( CellMessage request , Exception   exception ){

      }
      public void answerTimedOut( CellMessage request ){

      }
      public void getInfo( PrintWriter pw ){
          pw.println(" Module Args : "+ _core.getModuleArgs() ) ;
          pw.println("   Task Args : "+ _core.getTaskArgs() ) ;
      }
      public void timer(){
          //_log.info("Timer of "+_core.getName()+" triggered");
      }
      public String toString(){
         return "I'm "+_core.getName() ;
      }
   }


}
