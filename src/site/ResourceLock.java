package site;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import entity.RequestType;

import site.entity.LockType;

/**
 * Each entity of this class represents the lock of some resource. Contains lock
 * type. id of transactions involved, and the id of the resource
 * 
 * @author Daoan XU
 * 
 */
class ResourceLock {
    /**
     * The type of the lock
     */
    private String resource;
    private LockType type;
    private Set<String> transactions;

    /**
     * Set the input resource to this.resource. Set lock type to null. Set
     * transaction id to a empty HashSet
     * 
     * @param resource
     */
    ResourceLock(String resource) {
        this.resource = resource;
        this.type = null;
        this.transactions = new HashSet<String>();
    }

    String getResource() {
        return resource;
    }

    LockType getType() {
        return type;
    }

    Set<String> getTransactions() {
        return Collections.unmodifiableSet(transactions);
    }

    /**
     * addLock to this LockEntity. It should have been ensured by transaction
     * manager that the parameter are correct For secure reason, do double check
     * here to ensure security <br>
     * 
     * @param resource
     * @param transaction
     * @param type
     */
    void addLock(String resource, String transaction, LockType type) {
        /*
         * A lot duplicated Checks are done here Just to ensure security. if
         * everything is called in sequence, the easy one would be enough
         */

        // easyAddLock(resource, transaction, type);
        doubleCheckAddLock(resource, transaction, type);

    }

    private void easyAddLock(String resource, String transaction, LockType type) {
        // When this is called, every thing should be safe.
        // So:
        if (this.type == null) {
            this.type = type;
            this.transactions.add(transaction);
        } else if (this.type == LockType.WRITE || type == LockType.WRITE)
            this.type = LockType.WRITE;
        else
            this.transactions.add(transaction);

    }

    private void doubleCheckAddLock(String resource, String transaction,
            LockType type) {

        /*
         * this if should always be false. resources is a replicated data to
         * ensure that lock are given to correct resources
         */
        if (!resource.equals(this.resource)) {
            System.err.println("error: Attempting to add lock of [" + resource
                    + "] to a LockEntity of [" + this.resource + "]");
            return;
        }

        /*
         * if the current type is null, which means there is no lock now. safe
         * just check if the current transactions is empty
         */
        if (this.type == null && this.transactions.size() == 0) {
            this.type = type;
            if (this.type == LockType.RECOVERY)
                this.transactions.clear();
            else
                this.transactions.add(transaction);
            return;
        }

        /*
         * If current type is read, and the coming type is read, then OK
         */
        if (this.type == LockType.READ && type == LockType.READ) {
            this.transactions.add(transaction);
            return;
        }

        /*
         * If the current type or coming type contains Write, the coming
         * transaction must be the same as the ONLY transaction in the Set
         * 
         * This is a replicated conflict check.
         */
        if (this.type == LockType.WRITE || type == LockType.WRITE) {
            if (this.transactions.size() == 1
                    && this.transactions.contains(transaction)) {
                this.type = LockType.WRITE;
                return;
            }
        }

        /*
         * All other cases, there is error. Gives out error message.
         */

        System.err.println("error: site.LockEntity.addLock()\n LockEntity: "
                + this.toString() + "\nRequest:\n" + "Resource: " + resource
                + "Transaction ID: " + transaction + "type: " + type);

    }

    /**
     * Remove the lock of a transaction. Return true if at least one move is
     * successfully done.
     * 
     * @param resource
     * @param transaction
     */
    boolean removeLock(String resource, String transaction) {

        if (!this.isValid(resource))
            return false;

        if (this.type == LockType.RECOVERY) {
            this.type = null;
            return true;
        }

        if (this.type == null) {
            return false;
        }

        boolean returnValue = this.transactions.remove(transaction);
        if (this.transactions.isEmpty())
            this.type = null;
        return returnValue;
    }

    boolean removeLock(String resource, String transaction, LockType requestType) {
        // TODO
        return false;
    }

    void clear() {
        this.type = null;
        this.transactions.clear();
    }

    /**
     * Chech if this ojbect is valid, Rules: <br>
     * The resource must match the resource inside <br>
     * The tranactions set must be not null <br>
     * If there is no lock, that this.type == null, the transactions set must be
     * empty <br>
     * If the lock is read, the transactions set can not be empty <br>
     * If the lock is write, the transactions set must have one and only one
     * element <br>
     * If the lock is recovery, the transactions set msut be empty
     * 
     * @param resource
     * @return
     */
    boolean isValid(String resource) {

        if (!this.resource.equals(resource)) {
            System.err.println("warning: resource not match");
            return false;

        }

        if (this.transactions == null) {
            this.transactions = new HashSet<String>();
            System.err
                    .println("warning: unexpected null transactions, initialized now");
            return false;
        }

        if (this.type == null) {
            if (this.transactions.isEmpty())
                return true;

            this.transactions.clear();
            System.err
                    .println("warning: exists transactions in null type LockEntity, empty now");
            return false;
        }

        if (this.type == LockType.READ) {
            if (this.transactions.isEmpty()) {
                System.err
                        .println("warning: no transactions in read type LockEntity");
                return false;
            }

            return true;
        }

        if (this.type == LockType.WRITE) {
            if (this.transactions.size() == 1)
                return true;

            System.err
                    .println("warning: wrong transactions number in write type LockEntity");
            return false;
        }

        if (this.type == LockType.RECOVERY) {
            if (this.transactions.isEmpty())
                return true;

            this.transactions.clear();
            System.err
                    .println("warning: exists transactions in recovery type LockEntity, empty now");
            return false;
        }

        return false;

    }

    public String toString() {
        StringBuffer buff = new StringBuffer();
        buff.append("[Resource: ");
        buff.append(this.resource);
        buff.append(", Lock Type: ");
        buff.append(this.type);
        buff.append(", Transactions: ");
        for (String it : this.transactions) {
            buff.append(it);
            buff.append(", ");
        }
        buff.delete(buff.length() - 2, buff.length());
        buff.append("]");
        return buff.toString();
    }
}
