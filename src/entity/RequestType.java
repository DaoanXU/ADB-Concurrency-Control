package entity;

/**
 * This enum class is used for both rquest to transaction manager and request to
 * sites. Some of the name of the type may be used only for one part.
 * <br>
 * Used both: read, write, dump, abort
 * <br>
 * Used only for site: roread, commit, snapshot
 * <br>
 * Used only for transaction manager: fail, recover, begin, beginro, end
 * 
 * 
 * @author Daoan XU
 * @author jinglun dong
 */
public enum RequestType {
    READ, WRITE, ROREAD, FAIL, RECOVER, DUMP, COMMIT, ABORT, BEGIN, BEGINRO, END, SNAPSHOT
}
