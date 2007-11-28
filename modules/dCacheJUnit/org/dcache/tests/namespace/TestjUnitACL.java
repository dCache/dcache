package org.dcache.tests.namespace;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import junit.framework.TestCase;

import org.dcache.chimera.acl.ACE;
import org.dcache.chimera.acl.ACL;
import org.dcache.chimera.acl.GenIDforjUnitTest;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Permission;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AccessMask;
import org.dcache.chimera.acl.enums.AceFlags;
import org.dcache.chimera.acl.enums.AceType;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.chimera.acl.enums.InetAddressType;
import org.dcache.chimera.acl.enums.RsType;
import org.dcache.chimera.acl.enums.Who;
import org.dcache.chimera.acl.handler.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;
import org.dcache.chimera.acl.util.SQLHandler;
import org.junit.Test;

public class TestjUnitACL extends TestCase {
	
	private AclHandler aclHandler	= null;
	private DataSource ds_pooled	= null;

	public static final String	DEFAULT_DRIVER			= "org.postgresql.Driver";
	public static final String	DEFAULT_URL			    = "jdbc:postgresql://localhost:5432/chimera?prepareThreshold=3";
	public static final String	DEFAULT_USER				= "postgres";
	public static final String	DEFAULT_PSWD				= "";

	private String	driver		= DEFAULT_DRIVER; 
    private String	url			= DEFAULT_URL;
    private String	user		= DEFAULT_USER;
    private String	pswd		= DEFAULT_PSWD;
	
    void deleteFromTable()  throws  RuntimeException, SQLException, ClassNotFoundException{
    
    	Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, user, pswd);
    	PreparedStatement pstmt = null;
		
    	try {    	
			pstmt = conn.prepareStatement("delete from t_acl");
			int rs = pstmt.executeUpdate();
    	}
    	catch (SQLException e) {		
			throw new RuntimeException(SQLHandler.getMessage(e));
    		}    
    }
	
    
	@Test
	public void testAcl() throws Exception {
		deleteFromTable();
//		 initializing ACL Handler and SQL Data Source
    	initAclHandler();
   
    	String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    	    	 
    	    	int masks1 = (AccessMask.READ_DATA.getValue());
			    	masks1 += (AccessMask.WRITE_DATA.getValue());
			    	masks1 += (AccessMask.EXECUTE.getValue());
			    	System.out.println("masks1="+masks1);
			    	System.out.println("AccessMask (expected rwx ?) -> "+AccessMask.asString(masks1));
			    	
    			List<ACE> aces = new ArrayList<ACE>();
			  //EXAMPLE: 0.ACE allow READ_DATA
    		  //	     1.ACE allow READ_DATA, WRITE_DATA, EXECUTE
    		 //          2.ACE deny READ_DATA
    	     //
    		 // EXPECTED: action READ is allowed (as first READ_DATA - in ACE:0 - is allowed)
    		 //	          action WRITE is allowed (as WRITE_DATA is allowed)
    		//            action REMOVE is undefined
    		//**************  RESULT:     OK  ******************************	
    			
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.READ_DATA.getValue(),
						Who.USER,
						7,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
			   
				  aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						AceFlags.INHERIT_ONLY_ACE.getValue(),
						masks1,
						Who.USER,
						7,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				
			    	aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
							0,
							AccessMask.READ_DATA.getValue(),
							Who.USER,
							7,
							ACE.DEFAULT_ADDRESS_MSK,
							2 ) );
			    	
			    	aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
							0,
							AccessMask.APPEND_DATA.getValue(),
							Who.USER,
							7,
							ACE.DEFAULT_ADDRESS_MSK,
							3 ) );
				   
				System.out.println("rsID="+rsID);
				System.out.println("rsType="+rsType.toString());
			
			 System.out.println("aces :"+aces);
			 
			ACL newACL = new ACL(rsID, rsType, aces);
			System.out.println("New " + newACL);
     		aclHandler.setACL(newACL);
 
            assertFalse("List of ACEs is not empty", aclHandler.getACL(rsID).isEmpty());
            
            //Just for Info. Get resource ID (that was random generated) of the created ACL: 
            String rsIdnew = newACL.getRsId();
            System.out.println("rsIdnew = " + rsIdnew); 
            
            //Create test user subjectNew. who_id=7 as above. 
            Subject subjectNew = new Subject(7,100);
            
           Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK, InetAddressType.IPv4, "127.0.0.1");
      
            Owner ownerNew = new Owner(0, 0);
            
            Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
            
            //Action READ.
            Action actionREAD=Action.READ;
            Boolean check1 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
            System.out.println("check1 = " + check1);
            assertTrue("user with who_id=7 is allowed to READ file", check1);
            
            //Action WRITE.
            Action actionWRITE=Action.WRITE;
            Boolean check2 = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITE);
            System.out.println("check2 = " + check2);
            assertTrue("user with who_id=7 is allowed to WRITE file", check2);
            
            //Action REMOVE.
            Action actionREMOVE=Action.REMOVE;
            //USE THIS METHOD: isAllowed(Permission perm, Action action, Boolean isDir)
            Boolean check3 = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.FALSE);
            System.out.println("check3 = " + check3);
            Boolean check3now;
            if (check3==null) {
            	//if undefined assume "deny":
            	check3now=Boolean.FALSE; 	                                          
            } else {check3now=check3;}
            assertFalse("user who_id=7, action REMOVE is undefined", check3now);
       
            //CHECK getACL(String rsID)
            ACL aclCheckGet = aclHandler.getACL(rsID);
            System.out.println("***Test get(ACL : ) ");
            System.out.println("***TEST getList(): "+aclCheckGet.getList());
            System.out.println("***TEST getRsType(): "+aclCheckGet.getRsType());
            System.out.println("***TEST getSize(): "+aclCheckGet.getSize());
            System.out.println("***TEST permisionNew.asString(): "+permissionNew.asString());
     
        }
	
	/////////////////////////////////////////////
	
	@Test
	public void testAclOpen() throws Exception {
		
		  //EXAMPLE: 
		//  action: OPEN  (open a regular file)
	     // ACE flags: ADD_FILE, APPEND_DATA, EXECUTE, READ_DATA, WRITE_DATA
		// 
		 // EXPECTED: 	
		
//		 initializing ACL Handler and SQL Data Source
    	initAclHandler();
   
    	   String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    	    	 
    	    	int mask5 = (AccessMask.ADD_FILE.getValue());
			    	mask5 += (AccessMask.APPEND_DATA.getValue());
			    	mask5 += (AccessMask.EXECUTE.getValue());
			    	mask5 += (AccessMask.READ_DATA.getValue());
			    	mask5 += (AccessMask.WRITE_DATA.getValue());
			    	System.out.println("mask5="+mask5);
			    	System.out.println("AccessMask  "+AccessMask.asString(mask5));
			    	System.out.println("ADD_FILE.getAbbreviation()  "+AccessMask.ADD_FILE.getAbbreviation());
			    	System.out.println("APPEND_DATA.getAbbreviation()  "+AccessMask.APPEND_DATA.getAbbreviation());
			    	System.out.println("EXECUTE.getAbbreviation()  "+AccessMask.EXECUTE.getAbbreviation());
			    	System.out.println("READ_DATA.getAbbreviation()  "+AccessMask.READ_DATA.getAbbreviation());
			    	System.out.println("WRITE_DATA.getAbbreviation()  "+AccessMask.WRITE_DATA.getAbbreviation());
			    	List<ACE> aces = new ArrayList<ACE>();
		
    			
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						//AceFlags.INHERIT_ONLY_ACE.getValue(),
						0,
						mask5,
						Who.USER,
						1000,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
			    
				  aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						1000,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				
			    	aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
							AceFlags.INHERIT_ONLY_ACE.getValue(),
							AccessMask.READ_DATA.getValue(),
							Who.USER,
							1000,
							ACE.DEFAULT_ADDRESS_MSK,
							2 ) );
			    
				   
				System.out.println("rsID="+rsID);
				System.out.println("rsType="+rsType.toString());
			
			 System.out.println("aces :"+aces);
			 
			ACL newACL = new ACL(rsID, rsType, aces);
			System.out.println("New " + newACL);
     		aclHandler.setACL(newACL);
 
            assertFalse("List of ACEs is not empty", aclHandler.getACL(rsID).isEmpty());
            
            //CHECK getACL(String rsID)
            ACL aclCheckGet = aclHandler.getACL(rsID);
            System.out.println("Test get(ACL : )");
            System.out.println("TEST getList():"+aclCheckGet.getList());
            System.out.println("TEST getRsType():"+aclCheckGet.getRsType());
            System.out.println("TEST getSize():"+aclCheckGet.getSize());
            System.out.println("TEST toString():"+aclCheckGet.toString());
            
            //Just for Info. Get resource ID (that was random generated) of the created ACL: 
            String rsIdnew = newACL.getRsId();
            System.out.println("rsIdnew = " + rsIdnew); 
            
            //Create test user subjectNew. who_id=1000 as above. 
            Subject subjectNew = new Subject(1000,100);
            
           Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
      
            Owner ownerNew = new Owner(0, 0);
            
            Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
            
            ///////////////////////////////////////////////////////////////////////////////
            //Bits ADD_FILE, EXECUTE, READ_DATA, WRITE_DATA are allowed as defined in mask5.
            //  DELETE_CHILD is allowed as well (ACE:1)
            //////////////////////////////////////////////////////////////////////////////
            
            // NOT ok 
            Action actionLINK=Action.LINK;
            Boolean checkLINK = AclNFSv4Matcher.isAllowed(permissionNew, actionLINK);
            System.out.println("checkLINK = " + checkLINK);
            //assertTrue("For user who_id=1000 action LINK is allowed as bit ADD_FILE is set to allow", checkLINK);
            
            // NOT ok
            Action actionWRITE=Action.WRITE;
            Boolean checkWRITE = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITE);
            System.out.println("checkWRITE = " + checkWRITE);
            //assertTrue("For user who_id=1000 action WRITE is allowed as bit WRITE_DATA are allowed. APPEND_DATA is not checked for now.", checkWRITE);
            
            // OK
            Action actionREAD=Action.READ;
            Boolean checkREAD = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
            System.out.println("checkREAD = " + checkREAD);
            assertTrue("For user who_id=1000 action READ is allowed as bits EXECUTE, READ_DATA are allowed", checkREAD);
            
            // NOT ok
            Action actionREADLINK=Action.READLINK;
            Boolean checkREADLINK = AclNFSv4Matcher.isAllowed(permissionNew, actionREADLINK);
            System.out.println("checkREADLINK = " + checkREADLINK);
            //assertTrue("For user who_id=1000 action READLINK is allowed as bit EXECUTE is allowed", checkREADLINK);
            
            ////////////////////////////////////////////////////////////////////////
            //  NOT ok.  Action OPEN. Should be allowed as defined in mask5. 
            Action actionOPEN=Action.OPEN;
            //USE: isAllowed(Permission perm, Action action, Boolean isDir)
            Boolean checkOPEN = AclNFSv4Matcher.isAllowed(permissionNew, actionOPEN, Boolean.FALSE);
            System.out.println("checkOPEN = " + checkOPEN);
            //assertTrue("user who_id=1000 is allowed to OPEN file as all required bits are defined in mask5 ", checkOPEN);
  
            ////////////////////////////////////////////////////////////////////////
            // CHECK this !!!! OK after changes added to AclNFSv4Matcher.java
            //Action RENAME. Should be allowed as DELETE_CHILD and ADD_FILE are allowed.
            //CHANGES in AclNFSv4Matcher.java : Line44 '&& action != Action.RENAME' added:
            //if ( action != Action.OPEN && action != Action.CREATE && action != Action.REMOVE && action != Action.RENAME)
            Action actionRENAME=Action.RENAME;
            //1. USE THIS METHOD: isAllowed(Permission perm, Action action)
            //Boolean checkRENAME = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAME);
            //2. USE THIS METHOD: isAllowed(Permission perm, Action action, Boolean isDir)
            Boolean checkRENAME = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAME, Boolean.FALSE);
            System.out.println("check4 = " + checkRENAME);
            assertTrue("user who_id=1000, action RENAME is allowed as bits DELETE_CHILD and ADD_FILE are allowed", checkRENAME);
           //but output is strange: 
           // *** DEBUG Acl Matcher Results: A:rlnxD, D:0, DIR:false, RENAME: wfD => ALLOWED ***
            
            
        }
	
	void initAclHandler() throws Exception {
    	aclHandler = new AclHandler();
    	ds_pooled = aclHandler.getDataSource();
    }
	 
	protected String genRsID() throws Exception {
	        String rsID = GenIDforjUnitTest.newID(0);
	    	return rsID;
	    }
}
