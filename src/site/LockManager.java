package site;

import java.util.List;
import java.util.Set;

import site.entity.*;

/**
 * interface of LockManagers. <br>
 * <br>
 * The lockManager should only be used by Site. And It should be able to tell
 * the requesting lock will have conflict with the current locks. And it should
 * be able to create and remove locks. It should maintain the record of each
 * resource have what transaction have what lock on it. And should maintain the
 * record of each transaction have locks on what resources
 * 
 * @author Daoan XU
 * 
 */
interface LockManager {

    /**
     * Return true if the require resource is under recovery Otherwise return
     * false
     * 
     * @param resource
     * @return
     */
    boolean isRecoverying(String resource);

    /**
     * Assuming that the requering resource is not under recovery. Should have
     * check the recovery status before calling this method.
     * 
     * Return the conflict transactionIDs if there is transaction <br>
     * Return empty list if there is no conflict<br>
     * Return a set with only one element "Recovery" if there is a recovery lock
     * on the resource.
     * 
     * @param resource
     *            The resource to be checked
     * @param transactionID
     *            The transactionID to be checked
     * @param requestType
     *            The request type of the transaction on the resource. The value
     *            can be LockType.WRITE or LockType.READ
     * @return the conflict transactionIDs, empty Set means no conflict, null
     *         means error <br>
     *         if the resource is under recovery, return a set with only one
     * 
     */
    Set<String> checkConflict(String resource, String transactionID,
            LockType requestType);

    /**
     * set the Lock based on the Request given This should be called after
     * checkConfilct is called. Assuming that there is no Conflict.
     * 
     * @param resource
     *            The resource to be checked
     * @param transactionID
     *            The transactionID to be checked
     * @param requestType
     *            The request type of the transaction on the resource. The value
     *            can be LockType.WRITE or LockType.READ
     */
    void setLock(String resource, String transactionID, LockType requestType);

    /**
     * Add recovery Lock too all not Unique resources
     * 
     * @param notUnique
     *            the Set of not unique Resources
     */
    void recovery(Set<String> notUnique);

    /**
     * Remove all the locks on the site. Supposed only be used when site fails.
     */
    void removeAllLocks();

    /**
     * Remove The lock on a resource from a certain Transaction. Returns true if
     * at least one lock is removed.
     * 
     * @param resource
     * @param transactionID
     * @return true if there is some lock removed
     */
    boolean removeLock(String resource, String transactionID);
    
    /**
     * Remove The certain type of lock on a resource from a certain Transaction. Returns true if
     * at least one lock is removed.
     * 
     * @param resource
     * @param transactionID
     * @return true if there is some lock removed
     */
    boolean removeLock(String resource, String transactionID, LockType locktype);

    /**
     * Remove all locks given by the given Transaction Return true if at least
     * one lock is removed.
     * 
     * @param transaction
     * @return true if at least one lock is removed.
     */
    boolean removeLockByTransaction(String transaction);

    /**
     * Remove all locks on the given resource Return true if at least one lock
     * is removed.
     * 
     * @param resource
     * @return true if at least one lock is removed.
     */
    boolean removeLockByResource(String resource);

    /**
     * Remove all locks given by the given Transactions. Return true if at least
     * one lock is removed.
     * 
     * @param transaction
     * @return true if at least one lock is removed.
     */
    boolean removeLockByTransactions(Set<String> transactionIDs);

    /**
     * Remove all locks on the given resources. Return true if at least one lock
     * is removed.
     * 
     * @param resource
     * @return true if at least one lock is removed.
     */
    boolean removeLockByResources(Set<String> resources);

}