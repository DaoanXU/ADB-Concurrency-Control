package site;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author jinglun
 * 
 */
public class ImpDataManager {

    /**
     * data in DB, <resource, value> i.e. <x3, 30>
     */
    private Map<String, String> data;

    /**
     * include resource which shows only in this site
     */
    private final Set<String> unique;

    /**
     * writeLog contains dirty data which has not been committed, The key of
     * outer map is transaction ID. The inner map is <resource, value> pair
     */
    private HashMap<String, HashMap<String, String>> writeLog;

    /**
     * A read log, the key is transaction ID. The set contains resources name
     * which are accessed by given transaction. Doesn't contains log of read
     * only transaction.
     */
    private HashMap<String, Set<String>> readLog;

    /**
     *  Snapshot for read only transactions, clean data.
     */
    private HashMap<String, HashMap<String, String>> snapshot;

    /**
     * constructor of DataManager
     * 
     * @param data
     *            a HashMap of <variable, value> pairs
     * @param unique
     *            a set of resource name of unique variables
     */
    public ImpDataManager(Map<String, String> data, Set<String> unique) {
        this.data = data;
        this.unique = unique;
        writeLog = new HashMap<String, HashMap<String, String>>();
        readLog = new HashMap<String, Set<String>>();
        snapshot = new HashMap<String, HashMap<String, String>>();
    }

    Set<String> getReplicatedResource() {
        Set<String> result = new HashSet<String>(data.keySet());
        result.removeAll(unique);
        return result;
    }

    HashMap<String, Set<String>> getReadLog() {
        return readLog;
    }

    HashMap<String, HashMap<String, String>> getWriteLog() {
        return writeLog;
    }

    Map<String, String> getData() {
        return data;
    }

    HashMap<String, HashMap<String, String>> getSnapshot() {
        return snapshot;
    }

    Set<String> getUnique() {
        return unique;
    }

    /**
     * Log a write to writeLog
     * 
     * @param transaction
     *            Transaction ID as String
     * @param resource
     *            resource name as String
     * @param value
     *            new value of given write transaction
     */
    private void logTransaction(String transaction, String resource,
            String value) {
        if (!writeLog.containsKey(transaction)) {
            writeLog.put(transaction, new HashMap<String, String>());
        }
        HashMap<String, String> tmpLog = writeLog.get(transaction);
        tmpLog.put(resource, value);
        writeLog.put(transaction, tmpLog);
    }

    /**
     * Read value of given resource, return empty string if no such resource
     * 
     * @param transaction
     *            transaction ID as String
     * @param resource
     *            resource name
     * @param isReadOnly
     *            true if it is a read only transaction
     * @return
     */
    public String read(String transaction, String resource, boolean isReadOnly) {
        if (isReadOnly) {
            if (!snapshot.containsKey(transaction)) {
                throw new IllegalArgumentException(
                        "A read transaction which has no snapshot");
            }
            if (!snapshot.get(transaction).containsKey(resource)) {
                throw new IllegalArgumentException(
                        "snapshot doesn't contains resource: " + resource
                                + "for transaction: " + transaction);
            }
            return snapshot.get(transaction).get(resource);
        } else {
            // check if resource is in a write log
            if (this.writeLog.containsKey(transaction)) {
                if (writeLog.get(transaction).containsKey(resource)) {
                    // Add to read log
                    if (readLog.containsKey(transaction)) {
                        readLog.get(transaction).add(resource);
                    } else {
                        HashSet<String> tmpSet = new HashSet<String>();
                        tmpSet.add(resource);
                        readLog.put(transaction, tmpSet);
                    }
                    return writeLog.get(transaction).get(resource);
                }
            }
            // read from database directly
            if (this.data.get(resource) == null) {
                System.out.println(resource);
                throw new RuntimeException("no requested resource in this site");
            }
            // Add to read log
            if (readLog.containsKey(transaction)) {
                readLog.get(transaction).add(resource);
            } else {
                HashSet<String> tmpSet = new HashSet<String>();
                tmpSet.add(resource);
                readLog.put(transaction, tmpSet);
            }
            return this.data.get(resource);
        }
    }

    /**
     * A proxy to the writeLog
     * 
     * @param transaction
     * @param resource
     * @param value
     */
    public void write(String transaction, String resource, String value) {
        logTransaction(transaction, resource, value);
    }

    /**
     * Create a snapshot for read only transaction. This method should be called
     * when a new read only transaction is established.
     * 
     * @param transaction
     */
    public void createSnapshot(String transaction) {
        if (snapshot.containsKey(transaction)) {
            throw new IllegalArgumentException("snapshot already exist");
        }
        snapshot.put(transaction, new HashMap<String, String>(data));
    }

    /**
     * Write all data to database.
     * 
     * @param transaction
     * @return a set of resources that have been accessed by given transaction
     *         (R,W)
     */
    public Set<String> commit(String transaction) {
        if (writeLog.get(transaction) != null)
            this.data.putAll(writeLog.get(transaction));
        return terminateTransaction(transaction);
    }

    /**
     * clear write/read log and snapshot. Only keep the database unchanged.
     */
    public void fail() {
        writeLog = new HashMap<String, HashMap<String, String>>();
        readLog = new HashMap<String, Set<String>>();
        snapshot = new HashMap<String, HashMap<String, String>>();
    }

    /**
     * Terminate one given transaction, clear it's write and read log.
     * 
     * @param transaction
     * @return a set of resource which are accessed by given transaction
     */
    public Set<String> terminateTransaction(String transaction) {
        Set<String> result = new HashSet<String>();
        if (writeLog.containsKey(transaction)) {
            result.addAll(writeLog.get(transaction).keySet());
            writeLog.remove(transaction);
        }
        if (readLog.containsKey(transaction)) {
            result.addAll(readLog.get(transaction));
            readLog.remove(transaction);
        }
        return result;
    }

    /**
     * Dump all resources on this site
     * 
     * @author Daoan XU
     * @return resource name, value pair in string, and which is sorted by the
     *         the resource
     */
    public String dumpSite() {

        Map<String, String> tempM = new HashMap<String, String>(data);
        List<Map.Entry<String, String>> tempL = new LinkedList<Map.Entry<String, String>>();
        for (Map.Entry<String, String> entry : tempM.entrySet()) {
            tempL.add(entry);
        }

        Collections.sort(tempL, new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> o1,
                    Map.Entry<String, String> o2) {
                return Integer.valueOf(o1.getKey().substring(1)).compareTo(
                        Integer.valueOf(o2.getKey().substring(1)));
            }

        });

        return tempL.toString();

        // StringBuffer buffer = new StringBuffer();
        // for (Map.Entry<String, String> entry : data.entrySet()){
        // buffer.append(entry.getKey());
        // buffer.append(": ");
        // buffer.append(entry.getValue());
        // buffer.append(" ");
        // }
        // return buffer.toString();
    }

    /**
     * Dump value of given resource
     * 
     * @param resource
     * @return
     */
    public String dumpResource(String resource) {
        if (data.containsKey(resource)) {
            return resource + ": " + data.get(resource) + " ";
        } else {
            return resource + ": NULL ";
        }
    }

    /**
     * Check if requested resource is on this site
     * 
     * @param resource
     * @return
     */
    public boolean containsResource(String resource) {
        return data.containsKey(resource);
    }
}
