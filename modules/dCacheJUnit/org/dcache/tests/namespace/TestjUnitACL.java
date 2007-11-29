package org.dcache.tests.namespace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

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
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.enums.InetAddressType;
import org.dcache.chimera.acl.enums.RsType;
import org.dcache.chimera.acl.enums.Who;
import org.dcache.chimera.acl.handler.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;

public class TestjUnitACL {

    private static AclHandler aclHandler;
    private static Connection _conn;

    @BeforeClass
    public static void setUp() throws Exception {
        /*
         * init Chimera DB
         */

        Class.forName("org.hsqldb.jdbcDriver");

        _conn = DriverManager.getConnection("jdbc:hsqldb:mem:chimeraaclmem", "sa", "");

        File sqlFile = new File("modules/external/Chimera/sql/create-hsqldb.sql");
        StringBuilder sql = new StringBuilder();

        BufferedReader dataStr = new BufferedReader(new FileReader(sqlFile));
        String inLine = null;

        while ((inLine = dataStr.readLine()) != null) {
            sql.append(inLine);
        }

        Statement st = _conn.createStatement();

        st.executeUpdate(sql.toString());

        tryToClose(st);


        aclHandler = new AclHandler("modules/dCacheJUnit/org/dcache/tests/namespace/acl.properties");
    }

    @Test
    public void testAcl() throws Exception {
  
        String rsID =genRsID();
                RsType rsType = RsType.FILE;

                int masks1 = (AccessMask.READ_DATA.getValue());
                    masks1 += (AccessMask.WRITE_DATA.getValue());
                    masks1 += (AccessMask.EXECUTE.getValue());
                 

                List<ACE> aces = new ArrayList<ACE>();
              //EXAMPLE: 0.ACE allow READ_DATA
              //         1.ACE allow READ_DATA, WRITE_DATA, EXECUTE
             //          2.ACE deny READ_DATA
             //
             // EXPECTED: action READ is allowed (as first READ_DATA - in ACE:0 - is allowed)
             //           action WRITE is allowed (as WRITE_DATA is allowed)
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


            ACL newACL = new ACL(rsID, rsType, aces);

            aclHandler.setACL(newACL);

            //Check getACL()
            assertFalse("List of ACEs is not empty", aclHandler.getACL(rsID).isEmpty());

            //Get resource ID (that was random generated) of the created ACL, check equality:
            assertEquals(newACL.getRsId(),rsID);
            
            //Check equality of 'set'ed and 'get'ed  ACL.
            assertTrue("Test set/get", aclHandler.getACL(rsID).toString().equals(newACL.toString()));

   
            //Create test user subjectNew. who_id=7 as above.
            Subject subjectNew = new Subject(7,100);

            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK, InetAddressType.IPv4, "127.0.0.1");

            Owner ownerNew = new Owner(0, 0);

            Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);

            //Action READ.
            Action actionREAD=Action.READ;
            Boolean check1 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
            assertTrue("user with who_id=7 is allowed to READ file", check1);

            //Action WRITE.
            Action actionWRITE=Action.WRITE;
            Boolean check2 = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITE); 
            assertTrue("user with who_id=7 is allowed to WRITE file as WRITE_DATA is allowed", check2);

            //Action REMOVE. Undefined, expected NULL.
            Action actionREMOVE=Action.REMOVE;
            //USE THIS METHOD: isAllowed(Permission perm, Action action, Boolean isDir)
            Boolean check3 = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.FALSE); 
            assertNull("user who_id=7, action REMOVE is undefined", check3);
       

            //ALSO CHECK LOOUP "Lookup filename". Bit to check: EXECUTE
            Action actionLOOKUP=Action.LOOKUP;
            Boolean checkLOOKUP = AclNFSv4Matcher.isAllowed(permissionNew, actionLOOKUP);
            //System.out.println("checkLOOKUP = " + checkLOOKUP);
            assertTrue("user who_id=7 is allowed to LOOKUP filename as bit EXECUTE is allowed ", checkLOOKUP);
                 
        }

    /////////////////////////////////////////////

    @Test
    public void testAclOpen() throws Exception {

           //EXAMPLE.
           //  action: OPEN  (open a regular file)
           // ACE flags: ADD_FILE, APPEND_DATA, EXECUTE, READ_DATA, WRITE_DATA


           String rsID =genRsID();
                RsType rsType = RsType.FILE;

                int mask5 = (AccessMask.ADD_FILE.getValue());
                    mask5 = (AccessMask.APPEND_DATA.getValue());
                    mask5 += (AccessMask.EXECUTE.getValue());
                    mask5 += (AccessMask.READ_DATA.getValue());
                    mask5 += (AccessMask.WRITE_DATA.getValue());
                    
                    List<ACE> aces = new ArrayList<ACE>();

                   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
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
                            0,
                            AccessMask.READ_DATA.getValue(),
                            Who.USER,
                            1000,
                            ACE.DEFAULT_ADDRESS_MSK,
                            2 ) );

            ACL newACL = new ACL(rsID, rsType, aces);
            aclHandler.setACL(newACL);
           
    		//Check getACL()
            assertFalse("List of ACEs is not empty", aclHandler.getACL(rsID).isEmpty());

            //Get resource ID (that was random generated) of the created ACL, check equality:
            assertEquals(newACL.getRsId(),rsID);
            
            assertTrue("Test set/get", aclHandler.getACL(rsID).toString().equals(newACL.toString()));

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
            //System.out.println("checkLINK = " + checkLINK);
            boolean isAllowedOrNot = ( checkLINK != null && checkLINK == Boolean.TRUE); 
            assertTrue("For user who_id=1000 action LINK is allowed as bit ADD_FILE is set to allow", isAllowedOrNot);

            // NOT ok
            Action actionWRITE=Action.WRITE;
            Boolean checkWRITE = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITE);
            //System.out.println("checkWRITE = " + checkWRITE);
            assertTrue("For user who_id=1000 action WRITE is allowed as bit WRITE_DATA is allowed. APPEND_DATA is not checked for now.", checkWRITE);

            // OK
            Action actionREAD=Action.READ;
            Boolean checkREAD = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
            //System.out.println("checkREAD = " + checkREAD);
            assertTrue("For user who_id=1000 action READ is allowed as bits EXECUTE, READ_DATA are allowed", checkREAD);

            // NOT ok
            Action actionREADLINK=Action.READLINK;
            Boolean checkREADLINK = AclNFSv4Matcher.isAllowed(permissionNew, actionREADLINK);
            //System.out.println("checkREADLINK = " + checkREADLINK);
            boolean isAllowedOrNot2 = ( checkREADLINK != null && checkREADLINK == Boolean.TRUE); 
            assertTrue("For user who_id=1000 action READLINK is allowed as bit EXECUTE is allowed", isAllowedOrNot2);

            ////////////////////////////////////////////////////////////////////////
            //  NOT ok.  Action OPEN. Should be allowed as defined in mask5.
            Action actionOPEN=Action.OPEN;
            //USE: isAllowed(Permission perm, Action action, Boolean isDir)
            Boolean checkOPEN = AclNFSv4Matcher.isAllowed(permissionNew, actionOPEN, Boolean.FALSE);
            //System.out.println("checkOPEN = " + checkOPEN);
            assertTrue("user who_id=1000 is allowed to OPEN file as all required bits are defined in mask5 ", checkOPEN);

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
            //System.out.println("check4 = " + checkRENAME);
            assertTrue("user who_id=1000, action RENAME is allowed as bits DELETE_CHILD and ADD_FILE are allowed", checkRENAME);
           //but output is strange:
           // *** DEBUG Acl Matcher Results: A:rlnxD, D:0, DIR:false, RENAME: wfD => ALLOWED ***

          //ALSO CHECK LOOUP "Lookup filename". Bit to check: EXECUTE
            Action actionLOOKUP=Action.LOOKUP;
            Boolean checkLOOKUP = AclNFSv4Matcher.isAllowed(permissionNew, actionLOOKUP);
            System.out.println("checkLOOKUP = " + checkLOOKUP);
            assertTrue("user who_id=1000 is allowed to LOOKUP filename as bit EXECUTE is allowed ", checkLOOKUP);
            
        }

/////////////////////////////////////////////
	
	@Test
	public void testCREATEfileAllow() throws Exception {
	
    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue()+AccessMask.ADD_SUBDIRECTORY.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
			    
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             Action actionCREATE=Action.CREATE;
		             //USE: isAllowed(Permission perm, Action action, Boolean isDir) 
		             Boolean checkCREATE1 = AclNFSv4Matcher.isAllowed(permissionNew, actionCREATE, Boolean.TRUE);
		             //System.out.println("checkCREATE1 = " + checkCREATE1);
		             assertTrue("For user who_id=1001 action CREATE is allowed as bits ADD_FILE, ADD_SUBDIRECTORY are set to allow", checkCREATE1);
		         
		             //ALSO CHECK action LINK. Bit to check: ADD_FILE.
		             Action actionLINK=Action.LINK;
		             Boolean checkLINK1 = AclNFSv4Matcher.isAllowed(permissionNew, actionLINK);
		             //System.out.println("checkLINK1 = " + checkLINK1);
		             assertTrue("For user who_id=1001 action LINK is allowed: bit ADD_FILE is allowed", checkLINK1);
		             
		             
		           //ALSO check READ in case EXECUTE and READ_DATA undefined. Expected: deny. 
		           //boolean isAllowedOrNot is used to answer DENY if answer from ACLMatcher is UNDEFINED,
		           //i.e. if checkREAD5==null
		             Action actionREAD=Action.READ;
		             Boolean checkREAD5 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
		             boolean isAllowedOrNot = ( checkREAD5 != null && checkREAD5 == Boolean.TRUE); 
		             System.out.println("checkREAD5 = " + checkREAD5);
		             assertFalse("For who_id=1001 action READ is denied: bit EXECUTE and READ_DATA are undefined", isAllowedOrNot);
				}
	
/////////////////////////////////////////////
	
	@Test
	public void testCREATEfileDeny() throws Exception {
	
    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue()+AccessMask.EXECUTE.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				   
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.ADD_SUBDIRECTORY.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             Action actionCREATE=Action.CREATE;
		             //USE: isAllowed(Permission perm, Action action, Boolean isDir) 
		             Boolean checkCREATE2 = AclNFSv4Matcher.isAllowed(permissionNew, actionCREATE, Boolean.TRUE);
		             //System.out.println("checkCREATE2 = " + checkCREATE2);
		             assertFalse("For who_id=1001 action CREATE is denied: bit ADD_SUBDIRECTORY denied, ADD_FILE allowed", checkCREATE2);
		             
		           //Also check LOOUPP "Lookup parent directory". Bit to check: EXECUTE
		             Action actionLOOKUPP=Action.LOOKUPP;
		             Boolean checkLOOKUPP = AclNFSv4Matcher.isAllowed(permissionNew, actionLOOKUPP);
		             //System.out.println("checkLOOKUPP = " + checkLOOKUPP);
		             assertTrue("user who_id=1001 is allowed to LOOKUP filename as bit EXECUTE is allowed ", checkLOOKUPP);          
		             
		           //ALSO CHECK READLINK "Read symbolic link". Bit to check: EXECUTE
		             Action actionREADLINK=Action.READLINK;
		             Boolean checkREADLINK = AclNFSv4Matcher.isAllowed(permissionNew, actionREADLINK);
		             //System.out.println("checkREADLINK = " + checkREADLINK);
		             boolean isAllowedOrNot = ( checkREADLINK != null && checkREADLINK == Boolean.TRUE); 
		             assertTrue("user who_id=1001 is allowed to READLINK : bit EXECUTE is allowed ", isAllowedOrNot);
		             
				}
	
/////////////////////////////////////////////
	
	@Test
	public void testCREATEfileDeny2() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
   
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.ADD_SUBDIRECTORY.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
				   aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             Action actionCREATE=Action.CREATE;
		             //USE: isAllowed(Permission perm, Action action, Boolean isDir) 
		             Boolean checkCREATE3 = AclNFSv4Matcher.isAllowed(permissionNew, actionCREATE, Boolean.TRUE);
		             //System.out.println("checkCREATE3 = " + checkCREATE3);
		             assertFalse("For user who_id=1001 action CREATE is denied: bit ADD_SUBDIRECTORY allowed, ADD_FILE is set to deny", checkCREATE3);
		             
		             //ALSO CHECK action LINK. Bit to check: ADD_FILE is denied.
		             Action actionLINK2=Action.LINK;
		             Boolean checkLINK2 = AclNFSv4Matcher.isAllowed(permissionNew, actionLINK2);
		             System.out.println("checkLINK2 = " + checkLINK2);
		             assertFalse("For user who_id=1001 action LINK is denied: bit ADD_FILE is denied", checkLINK2);
		             
				}
/////////////////////////////////////////////
	
	@Test
	public void testREADallow() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.EXECUTE.getValue()+AccessMask.READ_DATA.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
	
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check READ. Bits: EXECUTE,READ_DATA 
		             Action actionREAD=Action.READ;
		             Boolean checkREAD1 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
		             //System.out.println("checkREAD1 = " + checkREAD1);
		             assertTrue("For who_id=1001 action READ is allowed: bits EXECUTE, READ_DATA are allowed", checkREAD1);

	}

/////////////////////////////////////////////
	
	@Test
	public void testREADdeny() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    	    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.EXECUTE.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
	
				
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.READ_DATA.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check READ. Bits: EXECUTE,READ_DATA 
		             Action actionREAD=Action.READ;
		             Boolean checkREAD2 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
		             //System.out.println("checkREAD2 = " + checkREAD2);
		             assertFalse("For who_id=1001 action READ is denied: bits EXECUTE allowed, READ_DATA denied", checkREAD2);

		          
	}
	
	
/////////////////////////////////////////////
	
	@Test
	public void testREADdeny2() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.EXECUTE.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
	
				
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.READ_DATA.getValue(),
						Who.USER,
						1001,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(1001,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check READ. Bits: EXECUTE,READ_DATA 
		             Action actionREAD=Action.READ;
		             Boolean checkREAD3 = AclNFSv4Matcher.isAllowed(permissionNew, actionREAD);
		             //System.out.println("checkREAD3 = " + checkREAD3);
		             assertTrue("For who_id=1001 action READ is allowed: bit EXECUTE denied, READ_DATA allowed", checkREAD3);
	          
	}

/////////////////////////////////////////////
	
	@Test
	public void testREADDIRallow() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.LIST_DIRECTORY.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
	
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check READDIR. Bits: LIST_DIRECTORY 
		             Action actionREADDIR=Action.READDIR;
		             Boolean checkREADDIR = AclNFSv4Matcher.isAllowed(permissionNew, actionREADDIR);
		             //System.out.println("checkREADDIR = " + checkREADDIR);
		             assertTrue("For who_id=111 action READDIR is allowed: bit LIST_DIRECTORY allowed", checkREADDIR);
	          
	}

	
/////////////////////////////////////////////
	
	@Test
	public void testREMOVEDirectoryAllow() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
							0,
							AccessMask.DELETE_CHILD.getValue(),
							Who.USER,
							111,
							ACE.DEFAULT_ADDRESS_MSK,
							0 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check REMOVE. Bits: DELETE_CHILD
		             Action actionREMOVE=Action.REMOVE;
		             Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.TRUE);
		             assertTrue("For who_id=111 action REMOVE is allowed: bit DELETE_CHILD allowed", checkREMOVE);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testREMOVEDirectoryDeny() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				  
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
							0,
							AccessMask.DELETE_CHILD.getValue(),
							Who.USER,
							111,
							ACE.DEFAULT_ADDRESS_MSK,
							1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check REMOVE. Bits: DELETE_CHILD
		             Action actionREMOVE=Action.REMOVE;
		             Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.TRUE);
		             assertFalse("For who_id=111 action REMOVE is denied: bit DELETE_CHILD denied", checkREMOVE);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testREMOVEFileAllow() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				  
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
							0,
							AccessMask.DELETE.getValue(),
							Who.USER,
							111,
							ACE.DEFAULT_ADDRESS_MSK,
							0 ) );
				   
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
							0,
							AccessMask.DELETE_CHILD.getValue(),
							Who.USER,
							111,
							ACE.DEFAULT_ADDRESS_MSK,
							1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check REMOVE (for file). Bits: DELETE
		             Action actionREMOVE=Action.REMOVE;
		             Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.FALSE);
		             assertTrue("For who_id=111 action REMOVE file is allowed: bit DELETE allowed", checkREMOVE);
	          
	}

/////////////////////////////////////////////
	
	@Test
	public void testREMOVEFileDeny() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.DELETE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				  
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
							0,
							AccessMask.DELETE_CHILD.getValue(),
							Who.USER,
							111,
							ACE.DEFAULT_ADDRESS_MSK,
							1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check REMOVE (for file). Bits: DELETE
		             Action actionREMOVE=Action.REMOVE;
		             Boolean checkREMOVE = AclNFSv4Matcher.isAllowed(permissionNew, actionREMOVE, Boolean.FALSE);
		             assertFalse("For who_id=111 action REMOVE file is denied: bit DELETE denied", checkREMOVE);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEDirAllow() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue()+AccessMask.ADD_FILE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				  
				   
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (directory). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEdir=Action.RENAME;
		             Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEdir, Boolean.TRUE);
		             assertTrue("For who_id=111 action RENAME directory is allowed: DELETE_CHILD, ADD_FILE allowed", checkRENAMEdir);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEDir2() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						  AccessMask.ADD_FILE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (directory). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEdir=Action.RENAME;
		             Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEdir, Boolean.TRUE);
		             assertFalse("For who_id=111 action RENAME directory is allowed: DELETE_CHILD allowed, ADD_FILE denied", checkRENAMEdir);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEDir3() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.DIR;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.ADD_SUBDIRECTORY.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (directory). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEdir=Action.RENAME;
		             Boolean checkRENAMEdir = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEdir, Boolean.TRUE);
		             System.out.println("checkRENAMEdir = " + checkRENAMEdir);
		             assertFalse("For who_id=111 action RENAME directory is denied: DELETE_CHILD allowed, ADD_SUBDIRECTORY denied", checkRENAMEdir);
	          
	}
	
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEfileDeny() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEfile=Action.RENAME;
		             Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEfile, Boolean.FALSE);
		             System.out.println("checkRENAMEfile1 = " + checkRENAMEfile);
		             assertFalse("For who_id=111 action RENAME file is denied: DELETE_CHILD allowed, ADD_FILE denied", checkRENAMEfile);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEfileDeny2() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEfile=Action.RENAME;
		             Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEfile, Boolean.FALSE);
		             System.out.println("checkRENAMEfile2 = " + checkRENAMEfile);
		             assertFalse("For who_id=111 action RENAME file is denied: DELETE_CHILD denied, ADD_FILE allowed", checkRENAMEfile);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testRENAMEfileDeny3() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.DELETE_CHILD.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.ADD_FILE.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check RENAME (file). Bits: DELETE_CHILD, ADD_FILE
		             Action actionRENAMEfile=Action.RENAME;
		             Boolean checkRENAMEfile = AclNFSv4Matcher.isAllowed(permissionNew, actionRENAMEfile, Boolean.FALSE);
		             System.out.println("checkRENAMEfile2 = " + checkRENAMEfile);
		             assertFalse("For who_id=111 action RENAME file is denied: DELETE_CHILD , ADD_FILE denied", checkRENAMEfile);
	          
	}
	
/////////////////////////////////////////////
	
	@Test
	public void testWRITEfofileDeny() throws Exception {

    	        String rsID =genRsID(); 
    	    	RsType rsType = RsType.FILE;
    
    	    	List<ACE> aces = new ArrayList<ACE>();

				   aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
						0,
						AccessMask.WRITE_DATA.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						0 ) );
				   
				 
				   aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
						0,
						AccessMask.APPEND_DATA.getValue(),
						Who.USER,
						111,
						ACE.DEFAULT_ADDRESS_MSK,
						1 ) );
				   
				   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_WEAK, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check WRITE. Bits: APPEND_DATA (not checked), WRITE_DATA
		             Action actionWRITEfile=Action.WRITE;
		             Boolean checkWRITEfile = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITEfile);
		             System.out.println("checkWRITEfile = " + checkWRITEfile);
		             assertFalse("For who_id=111 action RENAME file is denied: DELETE_CHILD , ADD_FILE denied", checkWRITEfile);
	          
	}
	
	  /////////////////////////////////////////////

    @Test
    public void testWRITEfileAllowed() throws Exception {

           String rsID =genRsID();
                RsType rsType = RsType.FILE;
                
                List<ACE> aces = new ArrayList<ACE>();
     
               //will not work for FILE:     
               //AccessMask.ADD_FILE.getValue()+AccessMask.EXECUTE.getValue()+AccessMask.READ_DATA.getValue()+AccessMask.WRITE_DATA.getValue()+AccessMask.APPEND_DATA.getValue(),
                    
               aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                        0,
                        AccessMask.EXECUTE.getValue()+AccessMask.READ_DATA.getValue()+AccessMask.WRITE_DATA.getValue()+AccessMask.APPEND_DATA.getValue(),
                        Who.USER,
                        111,
                        ACE.DEFAULT_ADDRESS_MSK,
                        0 ) );
    
        		   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check WRITE. Bits: APPEND_DATA (not checked), WRITE_DATA
		             Action actionWRITEfile2=Action.WRITE;
		             Boolean checkWRITEfile2 = AclNFSv4Matcher.isAllowed(permissionNew, actionWRITEfile2);
		             //System.out.println("checkWRITEfile2 = " + checkWRITEfile2);
		             assertTrue("For who_id=111 action WRITE file is allowed: WRITE_DATA allowed", checkWRITEfile2);
 
    }
	
    
	  /////////////////////////////////////////////

    @Test
    public void testSETATTRfileAllowed() throws Exception {

           String rsID =genRsID();
                RsType rsType = RsType.FILE;
                
                List<ACE> aces = new ArrayList<ACE>();
                    
                int //masks = (AccessMask.APPEND_DATA.getValue());
                masks = (AccessMask.WRITE_ATTRIBUTES.getValue());
                masks += (AccessMask.WRITE_ACL.getValue());
                masks += (AccessMask.WRITE_OWNER.getValue());
                
               aces.add(new ACE( AceType.ACCESS_ALLOWED_ACE_TYPE,
                        0,
                        masks,
                        Who.USER,
                        111,
                        ACE.DEFAULT_ADDRESS_MSK,
                        0 ) );
    
               aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                       0,
                       AccessMask.APPEND_DATA.getValue(),
                       Who.USER,
                       111,
                       ACE.DEFAULT_ADDRESS_MSK,
                       1 ) );
               
               aces.add(new ACE( AceType.ACCESS_DENIED_ACE_TYPE,
                       0,
                       AccessMask.WRITE_DATA.getValue(),
                       Who.USER,
                       111,
                       ACE.DEFAULT_ADDRESS_MSK,
                       2 ) );
               
        		   ACL newACL = new ACL(rsID, rsType, aces);
		     		aclHandler.setACL(newACL);
		     		
		     		Subject subjectNew = new Subject(111,100);
		            
		            Origin originNew = new Origin(AuthType.ORIGIN_AUTHTYPE_STRONG, InetAddressType.IPv4, "127.0.0.1");
		       
		             Owner ownerNew = new Owner(0, 0);
		             
		             Permission permissionNew=AclMapper.getPermission(subjectNew, originNew, ownerNew, newACL);
		             
		             //Check SETATTR (Attribute ACL). Access flag: WRITE_ACL
		             Action actionSETATTRfile=Action.SETATTR;
		             //USE: Boolean isAllowed(Permission perm, Action action, FileAttribute attribute) 
		             Boolean checkSETATTRfile = AclNFSv4Matcher.isAllowed(permissionNew, actionSETATTRfile, FileAttribute.FATTR4_ACL);
		             //System.out.println("checkSETATTRfile = " + checkSETATTRfile);
		             assertTrue("For who_id=111 action SETATTR (Attribute FATTR4_ACL) is allowed: WRITE_ACL allowed", checkSETATTRfile);
		             
		             //Check SETATTR (Attribute OWNER_GROUP). Access flag: WRITE_OWNER
		             Boolean checkSETATTRfile2 = AclNFSv4Matcher.isAllowed(permissionNew, actionSETATTRfile, FileAttribute.FATTR4_OWNER_GROUP);
		             //System.out.println("checkSETATTRfile2 = " + checkSETATTRfile2);
		             assertTrue("For who_id=111 action SETATTR (Attribute  FATTR4_OWNER_GROUP) is allowed: WRITE_OWNER allowed", checkSETATTRfile2);
 
		             //Check SETATTR (Attribute SIZE). Access flag: WRITE_DATA
		             Boolean checkSETATTRfile3 = AclNFSv4Matcher.isAllowed(permissionNew, actionSETATTRfile, FileAttribute.FATTR4_SIZE);
		             //System.out.println("checkSETATTRfile3 = " + checkSETATTRfile3);
		             assertFalse("For who_id=111 action SETATTR (Attribute  FATTR4_SIZE) is denied: WRITE_DATA denied", checkSETATTRfile3);
    }
    
    protected String genRsID() throws Exception {
            String rsID = GenIDforjUnitTest.newID(0);
            return rsID;
        }

    static void tryToClose(Statement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            // _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }
}