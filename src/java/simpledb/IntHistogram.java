package simpledb;
import java.util.*;
import java.lang.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
     int[] bucketList;
     int range;
     int maximum;
     int minimum;
     int count;
     int buckets;
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        bucketList = new int[buckets];
        for(int i=0;i<buckets;i++){
            bucketList[i]=0;
        }
        this.buckets=buckets;
        range = (int)Math.ceil((double)(max-min+1)/(double)buckets);
        maximum=max;
        minimum=min;
        count=0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v >= minimum && v<= maximum){
            count++;
            int bucketNum=(v-minimum)/range;
            bucketList[bucketNum]+=1;
        }
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double result=0.0;
        int bucketNum=(v-minimum)/range;
        int b_right=0;
        int b_left=minimum+bucketNum*range;
        int width=range;
        double b_f=0.0;
        double b_part=0.0;

        if(op==Predicate.Op.EQUALS || op==Predicate.Op.LIKE){
            if(v<minimum || v>maximum){
                return 0.0;
            }
            result=((double)bucketList[bucketNum]/(double)width)/(double)count;
        } else if(op==Predicate.Op.GREATER_THAN){
            if(v>=maximum){
                return 0.0;
            }
            if(v<minimum){
                return 1.0;
            }
            for(int i=bucketNum;i<buckets;i++){
                if(minimum+(i+1)*range-1>maximum){
                    b_right=maximum;
                    width=b_right-b_left+1;
                } else {
                    b_right=minimum+(i+1)*range-1;
                }
                b_f=(double)bucketList[i]/(double)count;
                if(i==bucketNum){
                    b_part=(double)(b_right-v)/(double)width;
                    result+=b_f*b_part;
                } else {
                    result+=b_f;
                }
            }
        } else if(op==Predicate.Op.LESS_THAN){
            if(v<=minimum){
                return 0.0;
            }
            if(v>maximum){
                return 1.0;
            }
            for(int i=bucketNum;i>-1;i--){
                if(minimum+(i+1)*range-1>maximum){
                    b_right=maximum;
                    width=b_right-b_left+1;
                } else {
                    b_right=minimum+(i+1)*range-1;
                }

                b_f=(double)bucketList[i]/(double)count;
                if(i==bucketNum){
                    b_part=(double)(v-b_left)/(double)width;
                    result+=b_f*b_part;
                } else {
                    result+=b_f;
                }
            }

        } else if(op==Predicate.Op.LESS_THAN_OR_EQ){
            if(v<minimum){
                return 0.0;
            }
            if(v>=maximum){
                return 1.0;
            }
            for(int i=bucketNum;i>-1;i--){
                if(minimum+(i+1)*range-1>maximum){
                    b_right=maximum;
                    width=b_right-b_left+1;
                } else {
                    b_right=minimum+(i+1)*range-1;
                }

                b_f=(double)bucketList[i]/(double)count;
                if(i==bucketNum){
                    b_part=(double)((v+1)-b_left)/(double)width;
                    result+=b_f*b_part;
                } else {
                    result+=b_f;
                }
            }
        } else if(op==Predicate.Op.GREATER_THAN_OR_EQ){
            if(v<= minimum){
                return 1.0;
            }
            if(v>maximum){
                return 0.0;
            }
            for(int i=bucketNum;i<buckets;i++){
                if(minimum+(i+1)*range-1>maximum){
                    b_right=maximum;
                    width=b_right-b_left+1;
                } else {
                    b_right=minimum+(i+1)*range-1;
                }
                b_f=(double)bucketList[i]/(double)count;
                if(i==bucketNum){
                    b_part=(double)(b_right-(v-1))/(double)width;
                    result+=b_f*b_part;
                } else {
                    result+=b_f;
                }
            }

        } else if(op==Predicate.Op.NOT_EQUALS){
            if(v>maximum || v<minimum){
                return 1.0;
            }

            result=((double)((double)count-(double)bucketList[bucketNum]/(double)width)/(double)count);
        }
        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        String result="";
        for(int i=0;i<buckets;i++){
            result+="bucket number "+i+" has "+bucketList[i]+" elements \n";
        }
        return result;
    
    }

}
