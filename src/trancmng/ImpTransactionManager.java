package trancmng;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import entity.Request;
import entity.RequestType;
import site.Site;
import trancmng.entity.tranStatus;
import trancmng.entity.transactionEntity;

/**
 * 
 * @see TransactionManager
 * @author Daoan XU
 * 
 */

public class ImpTransactionManager implements TransactionManager {

    /**
     * Contains all Site object in order
     */
    private Collection<Site> sites;
    /**
     * Map from site name to its reference
     */
    private Map<String, Site> siteMap;

    /**
     * Contains all the resources that available at least on one site.
     */
    private Set<String> resources;

    /**
     * A map from the resource to the set of sites that holds it
     */
    private Map<String, List<Site>> sitesAvaliable;

    /**
     * The Queue of waiting requests
     */
    private Queue<Request> waitingList;

    /**
     * A map from Site to the set of transactions that have visiting it.<br>
     * Should ensure that all transactions in the values sets are running. 
     */
    private Map<Site, Set<String>> visitingTrans;

    /**
     * A map from the transaction to its the information it holds
     * 
     * @see {@link transactionEntity}
     * 
     */
    private Map<String, transactionEntity> transInfo;

    /**
     * The only constructor of is implementation. To use this transaction
     * manager, the caller must generate the data of the sites and gives a map
     * from "site name" to "connection to the the site (which is interface to of
     * the site in this simulation)". And a set of all possible resources.
     * 
     * In this constructor, the constructor will initialize all fields. And will
     * ask all sites to know which sites hold which resource. And keep this
     * information as a Map in a private filed
     * 
     * @param siteMap
     *            Map from "site name" to "site reference"
     * @param resources
     *            Set of all possible "resources (variables) name"
     */
    public ImpTransactionManager(Map<String, Site> siteMap,
            Set<String> resources) {
        this.sites = siteMap.values();
        this.siteMap = siteMap;
        this.resources = resources;
        this.sitesAvaliable = new HashMap<String, List<Site>>();
        this.waitingList = new LinkedList<Request>();
        this.visitingTrans = new HashMap<Site, Set<String>>();
        for (Site site : sites)
            this.visitingTrans.put(site, new HashSet<String>());
        this.transInfo = new HashMap<String, transactionEntity>();
        this.init();
    }

    /**
     * Do initialization work. Should only be called by Constructor
     * 
     * The only Work done here in this implementation is for each resources, ask
     * what site contains it and store this information
     */
    private void init() {
        // Create the map from resource to Sites that contains it
        // and remove the resources that no site contains it
        Set<String> removeResources = new HashSet<String>();
        for (String resource : resources) {
            List<Site> temp = new LinkedList<Site>();
            for (Site site : sites)
                if (site.containsResource(resource))
                    temp.add(site);
            if (temp.size() > 0)
                sitesAvaliable.put(resource, temp);
            else
                removeResources.add(resource);
        }
        this.resources.removeAll(removeResources);
    }

    /**
     * Checking the time stamps of the transactions return if "transaction"
     * needs to aborted when it conflicts with the transactions in Set
     * "conflicts" <br>
     * return null if it just wait return one transaction that it conflicts with
     * and is older than it.
     * 
     * @param transaction
     *            The name of the transaction in String.
     * @param conflicts
     *            The set of name of the transactions that are conflict with
     *            parameter <i>transaction</i>
     * @return The name of the transaction that kills the coming transaction.
     *         Other wise null
     */
    private String needAbort(String transaction, Set<String> conflicts) {
        transactionEntity thisone = transInfo.get(transaction);
        for (String conflict : conflicts)
            if (thisone.timestamp > transInfo.get(conflict).timestamp)
                return conflict;
        return null;
    }

    /**
     * Check if the request is conflict with the conflict in the waiting list.
     * Error message will be print to system.out
     * 
     * <br>
     * Conflict Rules: <br>
     * begin beginro dump fail recover abort <br>
     * >> no conflict <br>
     * read only read <br>
     * >> no conflict <br>
     * normal read and write <br>
     * >> have lock conflict with request in the waiting list <br>
     * end <br>
     * >> when request of same transaction is in the waiting list <br>
     * other type of request should not appear here.
     * 
     * @param request
     * @return true if there is conflict.
     */
    private boolean conflictWithWaitingQueue(Request request) {
        switch (request.requestType) {
        case BEGIN:
        case BEGINRO:
        case DUMP:
        case FAIL:
        case RECOVER:
        case ABORT:
            return false;
        case READ:
            if (this.transInfo.get(request.transaction).isReadOnly())
                return false;
        case WRITE:
            for (Request waitingRequest : this.waitingList)
                if (waitingRequest.resource.equals(request.resource))
                    if (waitingRequest.requestType == RequestType.WRITE
                            || request.requestType == RequestType.WRITE) {
                        System.out.println("conflict with request : ["
                                + waitingRequest + "], going to waiting list");
                        return true;
                    }
        case END:
            for (Request waitingRequest : this.waitingList)
                if (waitingRequest.transaction.equals(request.transaction)) {
                    System.out.println("conflict with request : ["
                            + waitingRequest + "], going to waiting list");
                    return true;
                }
            return false;

        default:
            throw new IllegalArgumentException("Invalid request type: ["
                    + request.requestType + "]");

        }
    }

    /**
     * Check if the resource request by the request is available. Return true if
     * it exists. Other wise return false and print error message to System.out
     * 
     * @param request
     * @return true if resource exists
     */
    private boolean requestResourceExists(Request request) {
        // check if this resource is contained in some site
        if (!this.resources.contains(request.resource)) {
            System.out.println("error: no site hold the resources ["
                    + request.resource + "]");
            return false;
        }
        return true;
    }

    /**
     * Check if the transaction that doing the request has begun. Return true if
     * it exists. Other wise return false and print error message to System.out
     * 
     * @param request
     * @return true if transaction has begun
     */
    private boolean reqeustTransactionExists(Request request) {
        if (this.transInfo.containsKey(request.transaction))
            return true;

        System.out.println("error: transaction [" + request.transaction
                + "] have not begun");
        return false;
    }

    /**
     * Check if the transaction that doing the request is running. And is of
     * status Running. Return true if it exists. Other wise return false and
     * print error message to System.out
     * 
     * @param request
     * @return true if the transaction is running
     */
    private boolean requestTransactionLiving(Request request) {

        if (!this.reqeustTransactionExists(request))
            return false;

        // check the transaction status
        transactionEntity tempT = this.transInfo.get(request.transaction);

        switch (tempT.status) {
        case Running:
            return true;
        case Aborted:
            System.out.println("error: transaction [" + request.transaction
                    + "] have been aborted");
            return false;
        case Commited:
            System.out.println("error: transaction [" + request.transaction
                    + "] have been commited");
            return false;
        }
        return false;
    }

    /**
     * Overriding @see {@link #TransactionManager} <br>
     * 
     * This is the only public method. All call from outside should use this.<br>
     * This method handles the incoming new requests. Process one line at a
     * time.
     * 
     * This implementation only try the waiting listed request at the beginning
     * of each round. And will start to handle the new request after no rquest
     * in the waiting list is available to execute.
     * 
     */
    public void handleRequests(Queue<Request> requests) {

        if (this.waitingList.size() > 0) {
            System.out.println("Trying waiting requests:");
            for (Request request : this.waitingList)
                System.out.println(request);

            System.out.println("----------------------");
            this.handleWaitingList();
            System.out.println("----------------------");
        }

        System.out.println("Trying new requests:");
        Request tempR;
        while ((tempR = requests.poll()) != null) {
            System.out.println(">>trying rquest : [" + tempR + "]");
            this.handleRequest(tempR);
        }

        System.out.println("==========================================");

    }

    /**
     * this private method handles the waiting List. The It will first remove
     * all elements from the waiting list to a temporary Queue, and execute them
     * one by one.
     * 
     * If there is a end request successfully executed, this means there is
     * possible some lock released. Then we will try the waiting list again to
     * see if there is possible some other execution. <br>
     */

    /*
     * TODO It is possible that some request is executed before it should. e.g.
     * considering t2 have read lock on x, and waiting queue W(t1,x) end(t2)
     * W(t3,x), W(t3,x) will execute before W(t1,x). However, this situation is
     * rare
     */
    private void handleWaitingList() {
        boolean haveEnd = false;
        Queue<Request> tempQ = this.waitingList;
        this.waitingList = new LinkedList<Request>();
        Request request;
        while ((request = tempQ.poll()) != null) {
            System.out.println(">>trying rquest : [" + request + "]");
            if (this.handleRequest(request)
                    && request.requestType == RequestType.END) {
                haveEnd = true;
            }
        }

        if (haveEnd)
            this.handleWaitingList();
    }

    /**
     * This private method handles one single request. It will do pre-checks and
     * then call the methods that handles separate types of requests. <br>
     * return true if the requst is successfully handled. <br>
     * <br>
     * Supported request type: <br>
     * begin, beginRO, read, write, abort, end, dump, fail, recover *
     * 
     * @param request
     * @return true if this request is successfully handled.
     */
    private boolean handleRequest(Request request) {

        /*
         * Check if the coming request is conflict with waiting request. If so
         * put it to waiting list. No abort here to avoid over kill
         */
        if (this.conflictWithWaitingQueue(request)) {
            System.out
                    .println("conflict with waiting list, going to waiting list");
            this.waitingList.offer(request);
            return false;
        }

        switch (request.requestType) {
        case BEGIN:
        case BEGINRO:
            /*
             * Request check is done inside the beginRequest method And should
             * be done.
             */
            return this.beginRequest(request);

        case READ:
            // resource must exists, transaction must be running
            if (!requestResourceExists(request))
                return false;
            if (!requestTransactionLiving(request))
                return false;

            // check if the request is a read only request
            // call readOnlyread or normal read
            if (this.transInfo.get(request.transaction).isReadOnly())
                return this.readOnlyRequest(request);

            return this.readRequest(request);

        case WRITE:
            // resource must exists, transaction must be running
            if (!requestResourceExists(request))
                return false;
            if (!requestTransactionLiving(request))
                return false;
            return this.writeRequest(request);

        case ABORT:
            // Transaction must be running
            if (!this.requestTransactionLiving(request))
                return false;
            return this.abortRequest(request);

        case END:
            // Transaction must be running
            if (!this.requestTransactionLiving(request))
                return false;
            return this.endRequest(request);

        case DUMP:
            // Complicated situation, check is done inside method
            return this.dumpRequest(request);

        case FAIL:
            // site must not done, check is done inside method
            // site must exists, check is done inside method
            return this.failRequest(request);

        case RECOVER:
            // site must be done, check is done inside method
            // site must exists, check is done insde method
            return this.recoverRequest(request);
        }
        return false;
    }

    /**
     * 
     * Handle read only transaction read. Return true if handle success<br>
     * 
     * Presumptions: transaction exists, transaction living, resource exists.
     * 
     * @param request
     * @return true if handle success
     */
    private boolean readOnlyRequest(Request request) {

        // Presumption: transaction exists, transaction running
        // check each site that this transaction have snapshot,
        // [this.transInfo.get(request.transaction)] transaction Entity of the
        // transaction,
        for (Site site : this.transInfo.get(request.transaction).visitedSites) {

            // Presumption: resource exists.
            // check if the site have the resources.
            // [this.sitesAvaliable.get(request.resource)] is all the sites that
            // contains the resources
            if (!this.sitesAvaliable.get(request.resource).contains(site))
                continue;

            // check if the site is running
            if (!site.isRunning())
                continue;

            // check if the recourse is available on that site
            if (site.isRecovering(request.resource))
                continue;

            // send request to site and return true;
            System.out.println(site.exeRequest(new Request(request.resource,
                    request.transaction, RequestType.ROREAD, null)));
            return true;
        }

        System.out
                .println("["
                        + request.transaction
                        + "] is going into the wail list because there is no site have avaliable data currently");

        this.waitingList.add(request);
        return false;
    }

    /**
     * Handle normal transaction read. Return true if handle success<br>
     * 
     * Presumptions: transaction exists, transaction living, resource exists.
     * 
     * @param request
     * @return true if handle success
     */
    private boolean readRequest(Request request) {
        String resource = request.resource;

        // Presumption : resource exists.
        // try all sites that holds the key.
        // There should be at least one site that holds this key.
        for (Site site : sitesAvaliable.get(resource)) {

            // check if the site is running
            if (!site.isRunning())
                continue;

            // check if the resource on the site is available
            if (site.isRecovering(resource))
                continue;

            // Check if there is conflict
            Set<String> conflicts = site.checkConflict(request);

            // If there is conflict.
            // Do wait die
            if (conflicts.size() > 0) {
                System.out
                        .print("warning: There is conflict with current lockers. ");

                // Check wait die
                String tempS = needAbort(request.transaction, conflicts);

                if (tempS == null) {
                    System.out
                            .println("["
                                    + request.transaction
                                    + "] is going into the wail list. Current transactions holding locks : "
                                    + conflicts.toString());
                    this.waitingList.add(request);
                } else {
                    System.out.println("[" + request.transaction
                            + "] is aborted because it is conflict with ["
                            + tempS + "]");
                    this.abortRequest(new Request(null, request.transaction,
                            RequestType.ABORT, null));
                }
                return false;
            }

            // coming here means no conflict
            System.out.println(site.exeRequest(request));

            // add the current transaction to the visitor of the site
            // [this.visitingTrans.get(site)] is the visitor Set of the site
            this.visitingTrans.get(site).add(request.transaction);

            // Presumption : transaction exists, transaction running
            // add the current site to the visited of the transaction
            // [this.transInfo.get(request.transaction)] is the transaction
            // Entity of the transaction
            // [].visitedSites is the visited sites Set of the transaction.
            this.transInfo.get(request.transaction).visitedSites.add(site);
            return true;
        }

        // if reaching here it means there is no running sites that holds the
        // request

        System.out
                .println("["
                        + request.transaction
                        + "] is abourted because there is no site have avaliable data currently");
        // this.transInfo.get(request.transaction).status = tranStatus.Waiting;
        // this.waitingList.add(request);
        this.abortRequest(new Request(null, request.transaction,
                RequestType.ABORT, null));
        return false;
    }

    /**
     * Handle normal transaction read. Return true if handle success<br>
     * 
     * Presumptions: transaction exists, transaction running, resource exists.
     * 
     * @param request
     * @return true if handle success
     */
    private boolean writeRequest(Request request) {

        String resource = request.resource;

        boolean haveConflict = false;
        boolean needAbort = false;
        String older = null;
        Set<String> allConflicts = new HashSet<String>();

        // Presumption : resource exists
        // try all sites that holds the key
        // There should be at least one site that holds this key
        // Only checks if there is conflict with any living site
        for (Site site : sitesAvaliable.get(resource)) {

            // Check the site is running.
            if (!site.isRunning())
                continue;

            // Check if the site is recovering the resource.
            // If so, no conflict to this site.
            if (site.isRecovering(resource))
                continue;

            // Check if there is conflict
            Set<String> conflicts = site.checkConflict(request);
            allConflicts.addAll(conflicts);
            if (conflicts.size() > 0) {
                String tempS = needAbort(request.transaction, conflicts);
                if (tempS != null) {
                    needAbort = true;
                    older = tempS;
                }
                haveConflict = true;
            }
        }

        // if there is conflict
        if (haveConflict) {
            System.out
                    .print("warning: There is conflict with current lockers. ");

            if (needAbort) {
                System.out.println("[" + request.transaction
                        + "] is aborted because it is conflict with [" + older
                        + "]");
                this.abortRequest(new Request(null, request.transaction,
                        RequestType.ABORT, null));
            } else {
                System.out
                        .println("["
                                + request.transaction
                                + "] is going into the wail list. Current transactions holding locks : "
                                + allConflicts.toString());
                this.waitingList.add(request);
            }
            return false;
        }

        // reaching here means no conflict.
        boolean successfullWiteToOneSite = false;
        for (Site site : sitesAvaliable.get(resource)) {
            if (!site.isRunning())
                continue;

            site.exeRequest(request);

            // add the current transaction to the visitor of the site
            // [this.visitingTrans.get(site)] is the visitor Set of the site
            this.visitingTrans.get(site).add(request.transaction);

            // Presumption : transaction exists, transaction running
            // add the current site to the visited of the transaction
            // [this.transInfo.get(request.transaction)] is the transaction
            // Entity of the transaction
            // [].visitedSites is the visited sites Set of the transaction.
            this.transInfo.get(request.transaction).visitedSites.add(site);
            successfullWiteToOneSite = true;
        }

        if (!successfullWiteToOneSite) {
            System.out
                    .println("["
                            + request.transaction
                            + "] is going into the wail list because there is no site have avaliable data currently");
            this.waitingList.add(request);
        }

        return successfullWiteToOneSite;
    }

    /**
     * handle fail request. return true if the request is success
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean failRequest(Request request) {
        Site tempSite = this.siteMap.get(request.site);

        // Check if the site exists
        if (tempSite == null) {
            System.out.println("error: site [" + request.site
                    + "] does not exists");
            return false;
        }

        // Check if the site is running
        if (!tempSite.isRunning()) {
            System.out.println("warning : site [" + tempSite
                    + "] is already fail");
            return false;
        }

        // fail the site
        tempSite.fail();

        // abort the transactions that have visited the site.
        for (String transaction : this.visitingTrans.get(tempSite)) {
            // in site the visitingTrans ensures the existence and running
            this.abortRequest(new Request(null, transaction, RequestType.ABORT,
                    null));
        }
        return true;
    }

    /**
     * handle dump request. return true if the request is success
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean dumpRequest(Request request) {

        // check if the resource request exists.
        if (request.resource != null)
            if (!this.resources.contains(request.resource)) {
                System.out.println("error: Dump request resource ["
                        + request.resource + "] does not exists");
                return false;
            }

        // check if the site request exists.
        if (request.site != null)
            if (!this.siteMap.containsKey(request.site)) {
                System.out.println("error: Dump request site [" + request.site
                        + "] does not exists");
                return false;
            }

        // Dump all sites
        if (request.site == null) {
            for (Site site : sites) {
                // Check if the site is running
                if (site.isRunning())
                    System.out.println(site.exeRequest(request));
                else
                    System.out.println("Site [" + site.getSiteNum()
                            + "] == Fail");
            }
            return true;
        }

        // Dump one site
        Site site = this.siteMap.get(request.site);
        // Check if the site is running
        if (site.isRunning())
            System.out.println(site.exeRequest(request));
        else
            System.out.println("Site [" + site.getSiteNum() + "] == Fail");
        return true;
    }

    /**
     * handle recover request. return true if the request is success
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean recoverRequest(Request request) {

        if (request.site == null) {
            System.out.println("error: recovery request have no site");
            return false;
        }

        if (request.site != null)
            if (!this.siteMap.containsKey(request.site)) {
                System.out.println("error: recovery request site ["
                        + request.site + "] does not exists");
                return false;
            }

        if (this.siteMap.get(request.site).isRunning()) {
            System.out.println("error: recovery request site [" + request.site
                    + "] is running");
            return false;
        }

        this.siteMap.get(request.site).recover();
        return true;
    }

    /**
     * handle begin request. return true if the request is success
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean beginRequest(Request request) {

        // Check if the transaction name is used before
        if (this.transInfo.containsKey(request.transaction)) {
            System.out.println("error : transaction [" + request.transaction
                    + "] already exists");
            return false;
        }

        // Create new transaction Entity
        // Including final time stamp
        transactionEntity tempT = new transactionEntity(request.transaction,
                request.requestType == RequestType.BEGINRO);
        this.transInfo.put(tempT.name, tempT);

        // if the transaction is read only, create snapshot on all running sites
        if (request.requestType == RequestType.BEGINRO) {
            for (Site site : sites) {
                if (!site.isRunning())
                    continue;

                site.exeRequest(new Request(null, tempT.name,
                        RequestType.SNAPSHOT, null));
                tempT.visitedSites.add(site);
            }
        }
        return true;
    }

    /**
     * handle abort request. return true if the request is success <br>
     * 
     * Presumption : transaction exists, transaction running
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean abortRequest(Request request) {

        // Presumption : transaction exists
        transactionEntity tempT = this.transInfo.get(request.transaction);

        // remove all transaction requests in the waiting list.
        Set<Request> removing = new HashSet<Request>();
        for (Request wait : this.waitingList) {
            if (wait.transaction.equals(request.transaction))
                removing.add(wait);
        }
        this.waitingList.removeAll(removing);

        // clear site visiting record
        // clear site lock and buffer data
        for (Site site : tempT.visitedSites) {
            if (!site.isRunning())
                continue;
            site.exeRequest(request);
            this.visitingTrans.get(site).remove(tempT.name);
        }

        tempT.status = tranStatus.Aborted;
        tempT.visitedSites = Collections.unmodifiableSet(tempT.visitedSites);
        System.out
                .println("transaction [" + tempT.name + "] have been aborted");
        return true;
    }

    /**
     * handle end request. return true if the request is success <br>
     * 
     * @param request
     * @return true if the request is success
     */
    private boolean endRequest(Request request) {

        if (!this.requestTransactionLiving(request))
            return false;

        transactionEntity tempT = this.transInfo.get(request.transaction);

        for (Site site : tempT.visitedSites) {
            if (!site.isRunning())
                continue;
            
            //Commit to each visited running site
            site.exeRequest(new Request(null, request.transaction,
                    RequestType.COMMIT, null));
            
            //remove from visiting transaction set of the site
            this.visitingTrans.get(site).remove(tempT.name);
        }

        tempT.status = tranStatus.Commited;
        tempT.visitedSites = Collections.unmodifiableSet(tempT.visitedSites);
        System.out
        .println("transaction [" + tempT.name + "] have success comitted");
        return true;
    }

}
