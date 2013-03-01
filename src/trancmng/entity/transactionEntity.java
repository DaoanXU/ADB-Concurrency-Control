package trancmng.entity;

import java.util.HashSet;
import java.util.Set;

import site.Site;

import entity.TimeStamp;

/**
 * Stores information of a transaction.
 * See fields for detail.
 * @author Daoan XU *
 */
public class transactionEntity {
    
    /**
     * the literal of the transaction
     */
    public final String name;
    
    /**
     * the time stamp of the transaction
     */
    public final Integer timestamp;
    
    /**
     * if the transaction is read only
     */
    private final boolean readonly;
    
    /**
     * the transaction status, running, aborted, or committed
     */
    public tranStatus status;
    
    /**
     * transaction log
     */
    public StringBuffer log;
    
    /**
     * the reference of the sites that the transaction have visited.
     * When modifying this set, should always ensure that the transaction is running.
     */
    public Set<Site> visitedSites;
    
    public transactionEntity(String name,boolean readonly) {
        this.name = name;
        this.timestamp = TimeStamp.getit();
        status = tranStatus.Running;
        this.readonly = readonly;
        this.log = new StringBuffer();
        this.visitedSites = new HashSet<Site>();
    }
    
    
    /**
     * @return if the transaction is read only
     */
    public boolean isReadOnly(){
        return readonly;
    }
}
