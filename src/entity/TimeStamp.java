package entity;
/**
 * This class generates increasing integer, start form 1.
 * Used as time satmp.
 * @author Daoan XU
 *
 */
public class TimeStamp {
    private static int timestamp = 1;
    public static int getit(){
        return timestamp ++;
    }
}
