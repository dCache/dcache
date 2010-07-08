package dmg.cells.examples ;

import  java.awt.* ;
import  java.awt.event.* ;
import  java.util.Date ;
import  java.io.* ;

import  dmg.util.* ;
import  dmg.cells.nucleus.* ;
import  dmg.cells.network.* ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      WorksheetCell
       extends    jWorksheet
       implements Cell, ActionListener, CellEventListener {

   private CellNucleus _nucleus   = null ;
   private CellShell   _shell     = null ;
   private boolean     _shellMode = false ;
   private boolean     _talk      = true ;
   private String      _cellName  ;
   private String      _worksheetStore ;

   public WorksheetCell( String cellName ){
      super() ;

      //
      // the cell preparation
      //
      _cellName = cellName ;
      _nucleus  = new CellNucleus( this , _cellName ) ;
      _nucleus.export() ;
      //
      //  the awt preparations
      //
      setActionListener( this ) ;
      setFont( new Font( "Monospaced" , Font.PLAIN , 12 ) );
      _shell = new CellShell( _nucleus ) ;
      //
      // others
      //
      try{
        if( ( _worksheetStore = System.getProperty( "user.home" ) ) != null ){
           _worksheetStore = _worksheetStore+"/.worksheet-"+_cellName ;
        }else{
           _worksheetStore = ".worksheet-"+_cellName ;
        }
      }catch( SecurityException se ){
        _worksheetStore = null ;
      }



   }
   public void actionPerformed( ActionEvent event ){
      _nucleus.say( "Action : "+event.getActionCommand() ) ;
      Args args = new Args( event.getActionCommand() ) ;
      if( _shellMode ){
          if( ( args.argc() > 0 ) && args.argv(0).equals("exit") ){
             _shellMode = false ;
             setBackground( Color.white ) ;
          }else{
            try{
               answer( _shell.command( event.getActionCommand() ) ) ;
            }catch( Exception eee ){}
          }
          return ;
      }
      if( args.argc() < 1 )return ;
      String command = args.argv(0) ;
      if( command.equals( "send" ) ){
          if( args.argc() < 3 ){
              answer( " USAGE : send <cell> <message>\n" ) ;
              return ;
          }
          try{
             _nucleus.sendMessage(
                new CellMessage(
                     new CellPath( args.argv(1) ) ,
                     args.argv(2) )
             ) ;
          }catch( Exception nse ){
             answer( "Exception : "+nse+"\n" ) ;
          }

      }else if( command.equals( "save" ) ){
          if( args.argc() == 1 ){
             saveWorksheet( _worksheetStore );
          }else{
             saveWorksheet( args.argv(1) );
          }
      }else if( command.equals( "restore" ) ){
          if( args.argc() == 1 ){
             restoreWorksheet( _worksheetStore );
          }else{
             restoreWorksheet( args.argv(1) );
          }
      }else if( command.equals( "shell" ) ){
          if( args.argc() < 2 ){
              _shellMode = true ;
              setBackground( Color.orange ) ;
              answer("") ;
              return ;
          }
          try{
            answer( _shell.command( args.argv(1) ) ) ;
          }catch( Exception eee ){}
      }else if( command.equals( "help" ) ){
          StringBuffer sb = new StringBuffer() ;
          sb.append( "send <cell> <message> " ) ;
          sb.append( "# sends a message to the specified cell\n" ) ;
          sb.append( "shell <command>       " ) ;
          sb.append( "# forward a command to the shell interpreter\n" ) ;
          answer( sb.toString() );
      }else{
          answer( " Worksheet : Command not found : "+command+"\n" ) ;
          return ;
      }
   }
   public String toString(){
     return "Worksheet Cell "+_nucleus.getCellName() ;
   }
   public String getInfo(){
     return toString() ;
   }
   public void saveWorksheet( String filename ){
      try{
         DataOutputStream out = new DataOutputStream(
                                new FileOutputStream( filename ) ) ;
         out.writeUTF( getText() ) ;
         out.close() ;
      }catch( Exception ee ){
         _nucleus.say( "Problem saving "+filename+" : "+ee ) ;
      }
   }
   public void restoreWorksheet( String filename ){
      try{

         DataInputStream in = new DataInputStream(
                              new FileInputStream( filename ) ) ;
         setText( in.readUTF() );
         in.close();
      }catch( Exception ee ){
         _nucleus.say( "Problem saving "+filename+" : "+ee ) ;
      }
   }
   public void   messageArrived( MessageEvent me ){
     if( me instanceof LastMessageEvent )return ;

     CellMessage msg  = me.getMessage() ;
     Object      obj  = msg.getMessageObject() ;
     CellPath    addr = msg.getSourcePath() ;
     CellPath    dest = msg.getDestinationPath() ;
     if( _talk ){
        append( "\nMessage arrived\n ------------------\n") ;
        append( "  Source  : "+addr.toString()+"\n" ) ;
        append( "  Dest.   : "+dest.toString()+"\n" ) ;
        append( "  Class   : "+obj.getClass()+"\n" ) ;
        append( "  Umid    : "+msg.getUOID()+"\n" ) ;
        append( "  LastUmid: "+msg.getLastUOID()+"\n" ) ;
     }
     if( ! msg.isFinalDestination() ){
       if( _talk )append( "  Mode    : Not Final Destination\n" ) ;
       try{
          _nucleus.sendMessage( msg ) ;
       }catch( Exception nre ){
           append( "  Note    : Exception while sending "+nre+"\n" ) ;
       }
     }else{
       if( _talk )append( "  Mode    : Final Destination\n" ) ;
     }
     if( obj instanceof String ){
        String str = (String)obj ;
        if( ( str.length() > 0 ) && ( str.charAt(0) == '@' ) ){
           if( _talk )append( "  What    : "+str +"\n") ;
           else append( str +"\n") ;
           _execute( str ) ;
        }else{
           if( _talk )append( "  What    :\n"+str +"\n") ;
           else append( str +"\n") ;
        }
     }else if( obj instanceof PingMessage ){
        append( "Ping message : "+obj.toString() +"\n") ;
     }else if( obj instanceof CellRoute []  ){
        append( "Routing list arrived \n") ;
        CellRoute [] routeList = (CellRoute [] )obj ;
        for( int i = 0 ; i< routeList.length ; i++ ){
           append( "  "+routeList[i].toString()+"\n" ) ;
        }
     }else if( obj instanceof CellTunnelInfo []  ){
        append( "CellTunnelInfo list arrived \n") ;
        CellTunnelInfo [] tunnelList = (CellTunnelInfo [] )obj ;
        for( int i = 0 ; i< tunnelList.length ; i++ ){
           append( "  "+tunnelList[i].toString()+"\n" ) ;
        }
     }else if( obj instanceof Exception ){
        append( "  Excp.   : "+obj.toString() +"\n") ;
     }

   }
   private void _execute( String str ){
     Args args = new Args( str ) ;
     if( args.argc() < 3 )return ;
     try{
       if( args.argv(1).equals("font") ){
          int fontSize = new Integer( args.argv(2) ).intValue() ;
          setFont( new Font( "Monospaced" , Font.PLAIN , fontSize ) ) ;
       }else if( args.argv(1).equals("talk") ){
          if( args.argv(2).equals("none") )_talk= false ;
          else _talk=true ;
       }
     }catch( Exception e ){
        append( "  Exception   : "+e.getMessage() +"\n") ;
     }

   }
   public void   prepareRemoval( KillEvent ce ){
        _nucleus.say( " prepareRemoval "+ce ) ;
        try{ Thread.sleep(6000) ; }
        catch( InterruptedException ie ){}
   }
   public void   exceptionArrived( ExceptionEvent ce ){
     _nucleus.say( " exceptionArrived "+ce ) ;
   }
   public void cellCreated( CellEvent ce ){
     _nucleus.say( " cellCreated "+ce ) ;
     append( " cellCreated "+ce+"\n" ) ;
   }

   public void cellDied( CellEvent ce ){
     _nucleus.say( " cellDied "+ce ) ;
     append( " cellDied "+ce+"\n" ) ;
   }

   public void cellExported( CellEvent ce ){
     _nucleus.say( " cellExported "+ce ) ;
     append( " cellExported "+ce+"\n" ) ;
   }
   public void routeAdded( CellEvent ce ){
     _nucleus.say( " routeAdded "+ce ) ;
     append( " routeAdded "+ce+"\n" ) ;
   }
   public void routeDeleted( CellEvent ce ){
     _nucleus.say( " routeDeleted "+ce ) ;
     append( " routeDeleted "+ce+"\n" ) ;
   }


}
