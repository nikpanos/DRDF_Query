package gr.unipi.datacron.common.grid.variant3D;

public abstract class EncodingCurve {

    private int bits;
    protected int dimensions;

    public abstract long toIndex(int x[]);

    public abstract Cell toPoint(long value);


    public EncodingCurve(int bits, int dimensions) {
        super();
        this.bits = bits;
        this.dimensions = dimensions;
    }

    public int getBits() {
        return bits;
    }

    public void setBits(int bits) {
        this.bits = bits;
    }


}
