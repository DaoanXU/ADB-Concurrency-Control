package site;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import site.entity.LockType;

/**
 * This implementation overwrite write lock onto read lock. That each
 * transaction can have only one lock on one resources
 * 
 * <br> remove by (resource,transaction,type) is not supported
 * 
 * @see LockManager
 * @author Daoan XU
 * 
 */

class ImpLockManager implements LockManager {

    /**
     * Map that maps "resource" to "lockEntity of the resource" <br>
     * Containing all lock information on the site.
     */
    private Map<String, ResourceLock> locksOfR;

    /**
     * Map that maps from "transaction" to "resources" that it have lock on it
     * Contains access History.
     */
    private Map<String, Set<String>> resourcesOfT;

    public ImpLockManager() {
        this.locksOfR = new HashMap<String, ResourceLock>();
        this.resourcesOfT = new HashMap<String, Set<String>>();
    }

    @Override
    public boolean isRecoverying(String resource) {
        ResourceLock thisLock = locksOfR.get(resource);

        if (thisLock == null || thisLock.getType() != LockType.RECOVERY)
            return false;
        return true;
    }

    @Override
    public Set<String> checkConflict(String resource, String transaction,
            LockType requestType) {

        ResourceLock thisLock = locksOfR.get(resource);

        // There is no lock on this resource.
        if (thisLock == null || thisLock.getType() == null)
            return new HashSet<String>();

        LockType thisType = thisLock.getType();

        // This resource in under recovery
        if (thisType == LockType.RECOVERY) {
            System.err.println("error: site.ImpLockManager.chechConflict\n  ["
                    + resource + "] is under recovery");
            return null;
        }

        // If the current lock or the requesting lock contains WRITE
        if (thisType == LockType.WRITE || requestType == LockType.WRITE) {
            // If the are from the same transaction, then OK
            // else conflict.
            Set<String> temp = thisLock.getTransactions();
            if (temp.size() == 1 && temp.contains(transaction))
                return new HashSet<String>();
            else
                return thisLock.getTransactions();
        }

        if (thisType == LockType.READ && requestType == LockType.READ)
            return new HashSet<String>();

        System.err
                .println("error: site.ImpLockManager.chechConflict\n  when thislock is "
                        + thisLock
                        + "\nComing request is [Resource: "
                        + resource
                        + ", Lock Type: "
                        + requestType
                        + ", Transaction: " + transaction + "]");

        return null;
    }

    @Override
    public void setLock(String resource, String transaction,
            LockType requestType) {
        if (requestType != LockType.READ && requestType != LockType.WRITE) {
            System.err
                    .println("error: site.ImpLockManager.setLock\n  invalid coming request "
                            + requestType);
            return;
        }

        ResourceLock thisLock = locksOfR.get(resource);

        // If this resource is under Recovery
        if (thisLock != null && thisLock.getType() == LockType.RECOVERY) {
            if (requestType == LockType.READ)
                System.err
                        .println("error: site.ImpLockManager.setLock\n  when recoverying get read request, ["
                                + resource + "] is under recovery");

            if (!this.resourcesOfT.containsKey(transaction))
                this.resourcesOfT.put(transaction, new HashSet<String>());
            this.resourcesOfT.get(transaction).add(resource);
            return;
        }

        // There is no lock on this resource yet.
        if (thisLock == null) {
            thisLock = new ResourceLock(resource);
            locksOfR.put(resource, thisLock);
        }

        thisLock.addLock(resource, transaction, requestType);

        if (!this.resourcesOfT.containsKey(transaction))
            this.resourcesOfT.put(transaction, new HashSet<String>());
        this.resourcesOfT.get(transaction).add(resource);

    }

    @Override
    public void recovery(Set<String> notUnique) {
        this.locksOfR.clear();
        this.resourcesOfT.clear();

        for (String resource : notUnique) {
            ResourceLock tempLock = new ResourceLock(resource);
            tempLock.addLock(resource, null, LockType.RECOVERY);
            this.locksOfR.put(resource, tempLock);
        }
    }
    
    @Override
    public void removeAllLocks() {
        this.locksOfR.clear();
        this.resourcesOfT.clear();
    }

    @Override
    public boolean removeLock(String resource, String transaction) {
        ResourceLock rLock = locksOfR.get(resource);
        Set<String> tResources = this.resourcesOfT.get(transaction);

        if (rLock == null) {
            System.err
                    .println("error: site.ImpLockManager.removeLock\n  No lock on locksOfR of ["
                            + resource + "] found");
            return false;
        }

        if (tResources == null) {
            System.err
                    .println("error: site.ImpLockManager.removeLock\n  No resource on resourcesOfT of ["
                            + tResources + "] found");
            return false;
        }

        if (!rLock.isValid(resource))
            return false;

        boolean thereturn = false;
        thereturn = rLock.removeLock(resource, transaction);
        if (!tResources.contains(resource)) {
            System.err
                    .println("error: site.ImpLockManager.removeLock\n  resourcesOfT and locksOfR record mot match");
            return false;
        }
        return thereturn;
    }

    @Override
    public boolean removeLockByTransaction(String transaction) {

        Set<String> thisResources = this.resourcesOfT.get(transaction);

        // This transaction locks no resources
        if (thisResources == null || thisResources.isEmpty())
            return false;

        boolean thereturn = false;
        for (String resource : thisResources) {
            thereturn = this.removeLock(resource, transaction) || thereturn;
        }

        return thereturn;
    }

    @Override
    public boolean removeLockByResource(String resource) {
        ResourceLock thisLock = locksOfR.get(resource);

        // There is no lock on this resources
        if (thisLock == null)
            return false;

        locksOfR.remove(resource);

        if (!thisLock.isValid(resource))
            return false;

        // no lock on the resource
        if (thisLock.getType() == null)
            return false;

        for (String transaction : thisLock.getTransactions())
            this.resourcesOfT.get(transaction).remove(resource);
        thisLock.clear();
        return true;
    }

    @Override
    public boolean removeLockByTransactions(Set<String> transactions) {
        boolean thereturn = false;
        for (String transaction : transactions)
            thereturn = this.removeLockByTransaction(transaction) || thereturn;
        return thereturn;
    }

    @Override
    public boolean removeLockByResources(Set<String> resources) {
        boolean thereturn = false;
        for (String resource : resources)
            thereturn = removeLockByResource(resource) || thereturn;
        return thereturn;
    }

    @Override
    public boolean removeLock(String resource, String transactionID,
            LockType locktype) {
        throw new UnsupportedOperationException();
    }

}