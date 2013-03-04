// $Id: JCommander.java,v 1.1 2001-06-05 06:18:47 cvs Exp $
//
package dmg.cells.services.gui ;
//

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import dmg.cells.applets.login.DomainConnection;
import dmg.cells.applets.login.DomainConnectionListener;
import dmg.cells.applets.login.DomainEventListener;

public class      JCommander
       extends    JPanel
       implements DomainConnectionListener,
                  DomainEventListener {
   private static final long serialVersionUID = -8239106425185640530L;
   private DomainConnection _connection;
   private Font        _bigFont      = new Font( "Times" , Font.BOLD , 26 ) ;
   private JTextField  _commandField = new JTextField() ;
   private JTextArea   _displayArea  = new JTextArea() ;
   private JScrollPane _scrollPane;
   private JButton     _clearButton  = new JButton("Clear") ;
   private JTextField  _destination  = new JTextField() ;
   private JPanel createSouth(){
       GridBagLayout lo = new GridBagLayout() ;
       GridBagConstraints c = new GridBagConstraints()  ;
       JPanel panel = new JPanel( lo ) ;

       c.gridheight = 1 ;
       c.insets     = new Insets(4,4,4,4) ;

       c.gridwidth  = 1 ; c.gridx = 0 ; c.gridy = 0 ;
       panel.add( _clearButton , c ) ;
       c.gridwidth  = 1 ; c.gridx = 1 ; c.gridy = 0 ;
       panel.add( new JLabel("Destination") , c ) ;

       c.weightx = 1.0 ;
       c.weighty = 0.0 ;
       c.gridwidth  = 1 ; c.gridx = 2 ; c.gridy = 0 ;
       c.fill = GridBagConstraints.HORIZONTAL ;
       panel.add( _destination , c ) ;
       c.gridwidth  = 3 ; c.gridx = 0 ; c.gridy = 1 ;
       panel.add( _commandField , c ) ;

       JPanel jp = new JPanel( new BorderLayout() ) ;
       jp.add( panel , "Center" ) ;
       return jp ;
   }
   public JCommander( DomainConnection connection ){
      _connection = connection ;
      BorderLayout l = new BorderLayout() ;
      l.setVgap(10) ;
      l.setHgap(10);
      setLayout(l) ;
      _connection.addDomainEventListener(this) ;
      JLabel label = new JLabel( "Commander" , JLabel.CENTER ) ;
      label.setFont( _bigFont ) ;

      add( label , "North" ) ;
      _displayArea.setEditable(false);
      _scrollPane = new JScrollPane( _displayArea ) ;
      add( _scrollPane   , "Center" ) ;

      add( createSouth() , "South" ) ;

      _clearButton.addActionListener(
         new ActionListener(){
            @Override
            public void actionPerformed( ActionEvent event ){
               _displayArea.setText("");
            }
         }
      ) ;
      _commandField.addActionListener(

          new ActionListener(){
              @Override
              public void actionPerformed( ActionEvent event ){
                 String text = _commandField.getText() ;
                 _commandField.setText("");
                 try{
                    String destination = _destination.getText() ;
                    if( destination.equals("") ){
                       _connection.sendObject( text , new OurListener() , 4 ) ;
                    }else{
                       System.out.println("Sending to "+destination ) ;
                       _connection.sendObject( destination , text , new OurListener() , 4 ) ;
                    }
                 }catch(Exception ee ){
                    System.err.println("Error in sending : "+ee ) ;
                 }
              }
          }
      ) ;
   }
   private void append( String text ){
      _displayArea.append(text);
      SwingUtilities.invokeLater(

         new Runnable(){
            @Override
            public void run(){
                Rectangle rect = _displayArea.getBounds() ;
                rect.y = rect.height - 30 ;
                _scrollPane.getViewport().scrollRectToVisible( rect ) ;
            }
         }
     ) ;
   }
   private class OurListener implements DomainConnectionListener {
      @Override
      public void domainAnswerArrived( Object obj , int subid ){
//         System.out.println( "Answer ("+subid+") : "+obj.toString() ) ;
         append(obj.toString()+"\n");
      }
   }
   @Override
   public Insets getInsets(){ return new Insets(5,5,5,5) ; }
   @Override
   public void connectionOpened( DomainConnection connection ){
      System.out.println("Connection opened");
   }
   @Override
   public void connectionClosed( DomainConnection connection ){
      System.out.println("Connection closed" ) ;
   }
   @Override
   public void connectionOutOfBand( DomainConnection connection, Object obj ){
      System.out.println("Connection connectionOutOfBand "+obj ) ;
   }
   @Override
   public void domainAnswerArrived( Object obj , int subid ){
      System.out.println( "Answer ("+subid+") : "+obj.toString() ) ;
   }
}
