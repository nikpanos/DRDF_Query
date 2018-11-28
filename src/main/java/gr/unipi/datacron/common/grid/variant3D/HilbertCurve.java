package gr.unipi.datacron.common.grid.variant3D;

import com.google.uzaygezen.core.BitVector;
import com.google.uzaygezen.core.BitVectorFactories;
import com.google.uzaygezen.core.CompactHilbertCurve;

import java.util.List;

public class HilbertCurve extends EncodingCurve {

    CompactHilbertCurve chc;


    public HilbertCurve(int bits, int dimensions) {
        super(bits, dimensions);
        int nums_bits_x = bits / 2;
        int nums_bits_y = bits / 2;
        this.chc = new CompactHilbertCurve(new int[]{nums_bits_x, nums_bits_y});

    }


    public long toIndex(int xy[]) {
        List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
        BitVector[] p = new BitVector[bitsPerDimension.size()];
        for (int i = p.length; --i >= 0; ) {
            p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        p[0].copyFrom(xy[0]);
        p[1].copyFrom(xy[1]);
        BitVector chi = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());
        chc.index(p, 0, chi);
        return (chi.toLong());
    }

    public Cell toPoint(long value1) {
        List<Integer> bitsPerDimension = chc.getSpec().getBitsPerDimension();
        BitVector[] p = new BitVector[bitsPerDimension.size()];
        for (int i = p.length; --i >= 0; ) {
            p[i] = BitVectorFactories.OPTIMAL.apply(bitsPerDimension.get(i));
        }

        BitVector h = BitVectorFactories.OPTIMAL.apply(chc.getSpec().sumBitsPerDimension());
        h.copyFrom(value1);

        chc.indexInverse(h, p);
        Cell t = new Cell(p[0].toLong(), p[1].toLong(), 0l);
        return (t);
    }


    public static void main(String[] args) {

        int b = 4;
        HilbertCurve chc = new HilbertCurve(b, b);
        for (int j = 0; j < b; j++) {
            for (int i = 0; i < b; i++) {

                // long hilbert_value = chc.toIndex(j);
                //System.out.println(j+" "+i+" "+hilbert_value);

            }
        }

    }


}
