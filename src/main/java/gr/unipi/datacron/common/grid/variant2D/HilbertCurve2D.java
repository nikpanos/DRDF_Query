package gr.unipi.datacron.common.grid.variant2D;

import java.util.List;

import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

public class HilbertCurve2D{
	
   CompactHilbertCurve chc ;


   public HilbertCurve2D(int nums_bits_x,int nums_bits_y){
	   this.chc = new CompactHilbertCurve(new int[] {nums_bits_x, nums_bits_y});
     }


     public long toIndex(long x, long y)
     {
       List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
       BitVector[] p = new BitVector[bitsPerDimension.size()];
       for (int i = p.length; --i >= 0;) {
           p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
       }

       p[0].copyFrom(x);
       p[1].copyFrom(y);
       BitVector chi = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());
       chc.index(p, 0, chi);
       return (chi.toLong());
     }

     public Cell toPoint(long value1)
     {
       List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
       BitVector[] p = new BitVector[bitsPerDimension.size()];
       for (int i = p.length; --i >= 0;) {
           p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
       }

       BitVector h = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());
       h.copyFrom(value1);

       chc.indexInverse(h, p);
       Cell t = new Cell(p[0].toLong(), p[1].toLong());
       return(t);
     }


     public static void main(String[] args) {
    	 
    	 int b=4;
    	 HilbertCurve2D chc = new HilbertCurve2D(b,b) ;
    	 for (int j=0;j<b;j++)
    	 {
    		 for (int i=0;i<b;i++)
        	 { 
        	 
    			 long hilbert_value = chc.toIndex(j,i);
    		 		System.out.println(j+" "+i+" "+hilbert_value);
    		 			 
        	 }
    	 }
 		
     }

    	 
 }
