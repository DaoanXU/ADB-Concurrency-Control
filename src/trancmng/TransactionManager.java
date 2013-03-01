package trancmng;

import java.util.List;
import java.util.Queue;

import site.Site;

import entity.Request;

/**
 * Transaction Manager Class The only public method is handleRequst. All outer
 * requests must use this method.
 * 
 * Transaction Manager is only in charge of transaction manage. It should not in
 * charge of site initialization
 * 
 * @author Daoan XU
 * 
 */
public interface TransactionManager {
    /**
     * Handle the requests parsed by main server Get one line of input every
     * time.
     * 
     * @param request
     */
    public void handleRequests(Queue<Request> request);

}
