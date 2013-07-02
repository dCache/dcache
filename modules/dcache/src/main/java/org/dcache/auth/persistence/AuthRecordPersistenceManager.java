/*
 * AuthRecordPersistenceManager.java
 *
 * Created on July 1, 2008, 12:31 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.auth.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.Group;
import org.dcache.auth.GroupList;
import org.dcache.srm.SRMUserPersistenceManager;

/**
 *
 * @author timur
 */
public class  AuthRecordPersistenceManager implements SRMUserPersistenceManager{

    private Map<Long,AuthorizationRecord> authRecCache  =
        new HashMap<>();
    private static final Logger _logJpa =
            LoggerFactory.getLogger( AuthRecordPersistenceManager.class);
    EntityManager em ;
    public AuthRecordPersistenceManager(String propertiesFile) throws IOException {
        Properties p = new Properties();
        p.load(new FileInputStream(propertiesFile));
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("AuthRecordPersistenceUnit",p );
        em = emf.createEntityManager();
    }

    public AuthRecordPersistenceManager(String jdbcUrl,
    String jdbcDriver,
    String user,
    String pass) {
        _logJpa.debug("<init>("+jdbcUrl+","+
            jdbcDriver+","+
            user+","+
            pass+")");

        Properties p = new Properties();
        p.setProperty("javax.persistence.jdbc.driver", jdbcDriver);
        p.setProperty("javax.persistence.jdbc.url", jdbcUrl);
        p.setProperty("javax.persistence.jdbc.user", user);
        p.setProperty("javax.persistence.jdbc.password", pass);
        p.setProperty("datanucleus.connectionPoolingType", "BoneCP");
        p.setProperty("datanucleus.connectionPool.minPoolSize", "1");
        p.setProperty("datanucleus.connectionPool.maxPoolSize", "20");

        EntityManagerFactory emf =
            Persistence.createEntityManagerFactory("AuthRecordPersistenceUnit", p);
        em = emf.createEntityManager();
    }

    public synchronized AuthorizationRecord persist(AuthorizationRecord rec) {
        if(authRecCache.containsKey(rec.getId())) {
            return authRecCache.get(rec.getId());
        }
        EntityTransaction t = em.getTransaction();
        try{
            t.begin();
            // We assume that gPlazma guaranties that the records with the
            // same id will be always the same and unchanged, so
            // if rec is contained in the given entity manager,
            // no merge is needed
            if(!em.contains(rec)) {
                _logJpa.debug("em.contains() returned false");
                AuthorizationRecord rec1 =
                        em.find(AuthorizationRecord.class, rec.getId());
                if(rec1 == null) {
                    _logJpa.debug("em.find() returned null, persisting");
                    em.persist(rec);
                } else {
                    rec = rec1;
                }
            }
            t.commit();
        }finally
        {
            if (t.isActive())
            {
                t.rollback();
            }
            //em.close();
        }
        if(authRecCache.containsKey(rec.getId())) {
            return authRecCache.get(rec.getId());
        } else {
            authRecCache.put(rec.getId(),rec);
            return rec;
        }

    }

    @Override
    public synchronized AuthorizationRecord find(long id) {
        if(authRecCache.containsKey(id)) {
            return authRecCache.get(id);
        }
        AuthorizationRecord ar = null;
        EntityTransaction t = em.getTransaction();
        try{
            t.begin();
            _logJpa.debug(" searching for AuthorizationRecord with id="+id);
            ar = em.find(AuthorizationRecord.class, id);
            _logJpa.debug("found AuthorizationRecord="+ar);

            t.commit();
        }finally
        {
            if (t.isActive())
            {
                t.rollback();
            }

            //em.close();
        }
        if( ar == null) {
            return null;
        }
        if(authRecCache.containsKey(id)) {
            return authRecCache.get(id);
        } else {
            authRecCache.put(id,ar);
            return ar;
        }
    }



    public static void main(String[] args)
    {
        if(args == null || args.length == 0) {
            persistTest();
        } else {
            findTest(Long.parseLong(args[0]));
        }
    }

    public static void persistTest()
    {
        AuthRecordPersistenceManager pm =
            new AuthRecordPersistenceManager("jdbc:postgresql://localhost/testjpa",
            "org.postgresql.Driver","srmdcache","");
        Set<String> principals = new HashSet<>();
        principals.add("timur@FNAL.GOV");
        AuthorizationRecord ar =
            new AuthorizationRecord();
        ar.setId(System.currentTimeMillis());
        ar.setHome("/");
        ar.setUid(10401);
        ar.setName("timur");
        ar.setIdentity("timur@FNAL.GOV");
        ar.setReadOnly(false);
        ar.setPriority(10);
        ar.setRoot("/pnfs/fnal.gov/usr");

        GroupList gl1 = new GroupList();
        gl1.setAuthRecord(ar);
        Group group11 = new Group();
        Group group12 = new Group();
        Group group13 = new Group();
        group11.setGroupList(gl1);
        group12.setGroupList(gl1);
        group13.setGroupList(gl1);
        group11.setName("Group1");
        group11.setGid(1530);
        group12.setName("Group2");
        group12.setGid(1531);
        group13.setName("Group3");
        group13.setGid(1533);
        List<Group> l1 = new LinkedList<>();
        l1.add(group11);
        l1.add(group12);
        l1.add(group13);

        gl1.setAttribute(null);
        gl1.setGroups(l1);

        GroupList gl2 = new GroupList();
        gl2.setAuthRecord(ar);
        Group group21 = new Group();
        Group group22 = new Group();
        group21.setGroupList(gl2);
        group22.setGroupList(gl2);
        group21.setName("Group4");
        group21.setGid(2530);
        group22.setName("Group5");
        group22.setGid(2530);
        List<Group> l2 = new LinkedList<>();
        l2.add(group21);
        l2.add(group22);
        gl2.setAttribute(null);
        gl2.setGroups(l2);
        List<GroupList> gll = new LinkedList<>();
        gll.add(gl1);
        gll.add(gl2);
        ar.setGroupLists(gll);
        System.out.println("persisting "+ar);
        pm.persist(ar);
        System.out.println("persisted successfully ");
        System.out.println("id="+ar.getId());
    }

    public static void findTest(long id)
    {
        AuthRecordPersistenceManager pm =
            new AuthRecordPersistenceManager("jdbc:postgresql://localhost/testjpa",
            "org.postgresql.Driver","srmdcache","");
      // AuthRecordPersistenceManager pm =
      //      new AuthRecordPersistenceManager("/tmp/pm.properties");
        AuthorizationRecord ar = pm.find(id);
        if(ar == null) {
            System.out.println("AuthorizationRecord with id="+id +" not found ");
        }
        System.out.println(" found "+ar);

    }

}
