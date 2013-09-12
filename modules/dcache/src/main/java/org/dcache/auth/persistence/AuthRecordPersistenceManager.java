package org.dcache.auth.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;

import java.util.HashMap;
import java.util.Map;

import org.dcache.auth.AuthorizationRecord;

/**
 *
 * @author timur
 */
public class  AuthRecordPersistenceManager {

    private static final Logger _logJpa =
            LoggerFactory.getLogger( AuthRecordPersistenceManager.class);
    private final Map<Long,AuthorizationRecord> authRecCache  =
        new HashMap<>();
    private final EntityManager em;

    public AuthRecordPersistenceManager(EntityManagerFactory emf)
    {
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
}
