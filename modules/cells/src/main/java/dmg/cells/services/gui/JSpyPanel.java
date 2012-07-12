// $Id: JSpyPanel.java,v 1.1 2001-06-05 06:18:47 cvs Exp $
//
package dmg.cells.services.gui ;
//
import java.awt.*;
import java.awt.event.*;
import java.awt.font.*;
import javax.swing.*;
import java.util.*;
import java.io.* ;
import dmg.cells.applets.login.DomainConnection ;
import dmg.cells.applets.login.DomainConnectionListener ;
import dmg.cells.applets.login.DomainEventListener ;

import dmg.cells.network.* ;

public class      JSpyPanel
       extends    JPanel 
       implements DomainConnectionListener,
                  DomainEventListener {
                  
   private DomainConnection _connection   = null ;
   private JButton          _updateButton = new JButton("Update") ;
   private MovingPigs       _canvas       = new MovingPigs() ;
   private CanonicalTopology _topology    = null ;
   public JSpyPanel( DomainConnection connection ){
      _connection = connection ;
      BorderLayout l = new BorderLayout() ;
      l.setVgap(10) ;
      l.setHgap(10);
      setLayout(l) ;
      _connection.addDomainEventListener(this) ;
      
      _updateButton.addActionListener(
         new ActionListener(){
            @Override
            public void actionPerformed( ActionEvent event ){
               request() ;
            }
         } 
      ) ;
      
      add( _updateButton , "North" ) ;
      add( _canvas , "Center" ) ;

   }   
   private void request(){
      try{
         _connection.sendObject("topo","gettopomap",this,5);
      }catch(Exception ee ){
         System.err.println("Problem sending request : "+ee);
      }
   }
   @Override
   public Insets getInsets(){ return new Insets(5,5,5,5) ; }
   
   @Override
   public void connectionOpened( DomainConnection connection ){
      System.out.println("Connection opened");
      request() ;
   }
   @Override
   public void connectionClosed( DomainConnection connection ){
      System.out.println("Connection closed" ) ;
   }
   @Override
   public void connectionOutOfBand( DomainConnection connection, Object obj ){
      System.out.println("Connection connectionOutOfBand "+obj ) ;
   }
   private void display(){
      for( int i = 0 ; i < _topology.domains() ; i++ ){
         String domain = _topology.getDomain(i) ;
         System.out.println("New Domain : "+domain) ;
         _canvas.getItem( domain , true ) ;
      }
   }
   @Override
   public void domainAnswerArrived( Object obj , int subid ){
      System.out.println( "Answer ("+subid+") : "+obj.toString() ) ;
      
      CellDomainNode [] cdn = (CellDomainNode []) obj ;
      CanonicalTopology ct = new CanonicalTopology( cdn ) ;
      synchronized( this ){
         if( _topology == null ){
            _topology = ct ;
            display() ;
            return ;
         }
         if( _topology.equals( ct ) ) {
             return;
         }

         _topology = ct ;
         display();
      }
      
      return ;
   }
   
   
}
