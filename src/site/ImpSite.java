package site;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import entity.Request;
import entity.RequestType;


import site.entity.LockType;

/**
 * Site handles the request from the transaction manager
 * 
 * @author jinglun
 *
 */
public class ImpSite implements Site{
    
    private boolean isRunning;
    
    private ImpLockManager lockManager;
    
    private ImpDataManager dataManager;
    
    private int siteNum;
    
    
    /**
     * Constructor of Site
     * @param siteNum For the test scenario, siteNum is from 1 to 10
     * @param data data represented by String. No necessary to parse to int.
     * @param unique a set of resource name which only stored on this site
     */
    public ImpSite(int siteNum, Map<String, String> data, Set<String> unique){      
        this.siteNum = siteNum;
        lockManager = new ImpLockManager();
        dataManager = new ImpDataManager(data, unique);
        this.isRunning = true;
    }
   
    
    @Override
    public Set<String> checkConflict(Request request){
        if (!this.isRunning){
            throw new RuntimeException("access a down site");
        }        
        switch (request.requestType){
        case READ:
            return lockManager.checkConflict(request.resource, 
                    request.transaction, LockType.READ);            
        case WRITE:
            return lockManager.checkConflict(request.resource, 
                    request.transaction, LockType.WRITE);
        case ROREAD:
            return new HashSet<String>();
        default:
            throw new IllegalArgumentException("request type must be either read or write");
        }
    }

    
    @Override
    public String exeRequest(Request request) {
//        System.out.print("site [" + this.siteNum + "] == ");
//        System.out.println(request);
        if (!this.isRunning){
            throw new RuntimeException("access a down site");
        }     
        RequestType requestType = request.requestType;
        String result = "";
        switch (requestType){
        case READ:
            lockManager.setLock(request.resource, request.transaction, LockType.READ);
            result = "site [" + this.siteNum + "] == " + dataManager.read(request.transaction, request.resource, false);
            break;
        case WRITE:
            lockManager.setLock(request.resource, request.transaction, LockType.WRITE);
            if (request.value == null || request.value.isEmpty()){
                throw new IllegalArgumentException("value to be written to database is null");
            }
            dataManager.write(request.transaction, request.resource, request.value);
            break;
        case ROREAD:    //read issued by a read only transaction
            result = dataManager.read(request.transaction, request.resource, true);
            break;
        case DUMP:
            if (request.resource == null || request.resource.isEmpty()){
                result = "site [" + this.siteNum + "] == " + dataManager.dumpSite();
            }
            else{
                result = "site [" + this.siteNum + "] == " + dataManager.dumpResource(request.resource);
            }
            break;
        case COMMIT:
            if (request.transaction == null || request.transaction.isEmpty()){
                throw new IllegalArgumentException("transaction is null");
            }
            lockManager.removeLockByTransaction(request.transaction);
            dataManager.commit(request.transaction);
            break;
        case ABORT:
            if (request.transaction == null || request.transaction.isEmpty()){
                throw new IllegalArgumentException("transaction is null");
            }
            lockManager.removeLockByTransaction(request.transaction);
            dataManager.terminateTransaction(request.transaction);
            break;
        case SNAPSHOT:
            this.createSnapshot(request.transaction);
            break;
        default:
            throw new IllegalArgumentException("request type not supported");
        }        
        return result;        
    }

    
    @Override
    public void fail() {
        if (!this.isRunning){
            throw new RuntimeException("fail a site already down");
        }     
        this.isRunning = false;    
        this.lockManager.removeAllLocks();
    }
    

    @Override
    public boolean isRunning(){
        return this.isRunning;
    }


    @Override
    public int getSiteNum() {
        return siteNum;
    }


    ImpLockManager getLockManager() {
        return lockManager;
    }


    ImpDataManager getDataManager() {
        return dataManager;
    }


    @Override
    public boolean containsResource(String resource) {
        return dataManager.containsResource(resource);
    }


    @Override
    public boolean isRecovering(String resource) {
        return this.getLockManager().isRecoverying(resource);
    }


    @Override
    public void createSnapshot(String transaction) {
        this.getDataManager().createSnapshot(transaction);        
    }


    @Override
    public void recover() {
        this.lockManager.recovery(this.dataManager.getReplicatedResource());
        this.isRunning = true;
    }
}