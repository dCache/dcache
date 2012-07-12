// $Id: JRealm.java,v 1.1 2002-04-03 15:00:51 cvs Exp $
//
package dmg.cells.services.gui.realm ;
//
import java.awt.*;
import java.awt.event.*;
import java.awt.font.*;
import javax.swing.*;
import javax.swing.event.*;
import java.util.*;
import java.io.* ;
import dmg.cells.applets.login.DomainConnection ;
import dmg.cells.applets.login.DomainConnectionListener ;
import dmg.cells.applets.login.DomainEventListener ;

public class      JRealm 
       extends    JPanel {  
       
   private DomainConnection _connection = null ;
   private JCellPanel       _cellPanel  = null ;
   
   private class PrivateInfoPanel extends JPanel {
      @Override
      public Insets getInsets(){ return new Insets(5,5,5,5) ; }
      private PrivateInfoPanel(){
         setBorder( BorderFactory.createTitledBorder(
             BorderFactory.createCompoundBorder(
                 BorderFactory.createEmptyBorder(8,8,8,8) ,
                 BorderFactory.createLineBorder(Color.black) ) ,
             "JPanel" ) ) ;
 
      }
   }
   public JRealm( DomainConnection connection ){
      _connection = connection ;
      BorderLayout l = new BorderLayout() ;
      l.setVgap(10) ;
      l.setHgap(10);
      setLayout(l) ;
         setBorder( BorderFactory.createTitledBorder(
             BorderFactory.createCompoundBorder(
                 BorderFactory.createEmptyBorder(20,20,20,20) ,
                 BorderFactory.createLineBorder(Color.black) ) ,
             "Realm Controller" ) ) ;
      JScrollPane        sp = new JScrollPane();
      sp.setPreferredSize(new Dimension(300, 300));
      
      CellDomainTree tree = new CellDomainTree(_connection) ;
      tree.addTreeSelectionListener( new CellSelection() ) ;
      
      sp.getViewport().add( tree );
      
      add("West" , sp ) ;
      
      _cellPanel = new JCellPanel(_connection) ;
      add("Center" , _cellPanel ) ;

      
   }
//   public Insets getInsets(){ return new Insets(20,20,20,20) ; }
   
   private class CellSelection implements TreeSelectionListener {
      @Override
      public void valueChanged( TreeSelectionEvent event ){
         Object obj = event.getPath().getLastPathComponent() ;
         System.out.println( "Selection : "+obj.getClass().getName() ) ;
            
         if( ! ( obj instanceof CellDomainTree.CellNode ) ) {
             return;
         }
         CellDomainTree.CellNode cellNode = (CellDomainTree.CellNode) obj ;
         
         _cellPanel.setCell( cellNode.getAddress() , cellNode.getCellInfo() ) ;
      }
   }

}
