package gr.unipi.datacron.common.grid.variant3D;

import java.util.Arrays;

public class Grid {

    //x==lat and y==long
    protected int numberOfbits;
    protected int sizex;
    protected int sizey;
    protected int sizez;
    //Elaxistes times lat kai long
    protected double[] universeLowCorner;
    //Megistes times lat kai long
    protected double[] universeUpperCorner;

    //optional keeps the total number of points
    protected int[] countersArray;

    protected int dimensions;
    protected int curveType;

    public Grid() {
    }

    public Grid(int numberOfbits, double[] universeLowCorner, double[] universeUpperCorner, int dimensions, int curveType) {

        this.numberOfbits = numberOfbits;
        this.universeLowCorner = universeLowCorner;
        this.universeUpperCorner = universeUpperCorner;
        this.countersArray = new int[this.getNumberOfCells()];
        this.curveType = curveType;
        this.dimensions = dimensions;
    }


    public Cell code2cell(long value) {
        //HilbertCurve2D chc = new HilbertCurve2D((int)this.numberOfbits/2,(int)this.numberOfbits/2) ;
        EncodingCurve chc;
        if (this.curveType != 2) chc = new HilbertCurve(numberOfbits, dimensions);
        else
            chc = new ZorderCurve(numberOfbits, dimensions);

        Cell res = chc.toPoint(value);
        return res;
    }

    public long cell2code(int a[]) {
        EncodingCurve chc;
        if (this.curveType != 2) chc = new HilbertCurve(numberOfbits, dimensions);
        else
            chc = new ZorderCurve(numberOfbits, dimensions);
        //HilbertCurve2D chc = new HilbertCurve2D((int)this.numberOfbits/2,(int)this.numberOfbits/2) ;
        return chc.toIndex(a);
    }

    private long normalize_entity(double value, double old_min,
                                  double old_max, double new_min, double new_max) {
        // first we bring the values to 0,1
        double range = old_max - old_min;
        double result1 = (value - old_min) / range;
        //Then scale to [new_min,new_max]:
        double range2 = new_max - new_min;
        double normalized = (result1 * range2) + new_min;
        return (long) normalized;
    }


    private long getNormalizedLongitude(double x) {
        return normalize_entity(x, universeLowCorner[1],
                universeUpperCorner[1], 0, sizex);
    }

    private long getNormalizedLatitude(double y) {
        return normalize_entity(y, universeLowCorner[0],
                universeUpperCorner[0], 0, sizey);
    }

    private long getNormalizedHeight(double z) {
        return normalize_entity(z, universeLowCorner[2],
                universeUpperCorner[2], 0, sizez);
    }

    /*	public double getDenormalizedLongitude(double normalized_lon){
        return normalize_entity(normalized_lon, 0, sizex,universeLowCorner[0],
                universeUpperCorner[0]);
    }

    public double getDenormalizedLatitude(double normalized_lat){
        return normalize_entity(normalized_lat, 0, sizey,universeLowCorner[1],
                universeUpperCorner[1]);
    }
     */
    public double lengthDimReal(int i) {
        double temp = this.universeUpperCorner[i] - this.universeLowCorner[i];
        //System.out.println("lengthreal "+this.universeUpperCorner[i]+" "+this.universeLowCorner[i]+" "+temp);
        return temp;
    }

    public double getMaxValueReal(int i) {
        return this.universeUpperCorner[i];
    }

    public int getNumberOfCells() {
        return (int) Math.pow(2, numberOfbits);
    }

    public void setAggregatedCounters(int[] counters) {
        if (this.countersArray == null) {
            this.countersArray = new int[this.getNumberOfCells()];
            for (int i = 0; i < countersArray.length; i++) {
                countersArray[i] = 0;
            }
        }
        for (int i = 0; i < counters.length; i++) {
            countersArray[i] = countersArray[i] + counters[i];
        }
    }

    //returns the coordinates ij (integers row and column) of the cell
    public int[] getCell(SpatioTemporalObj obj) {
        long norm_lon = getNormalizedLongitude(obj.getLongitude());
        long norm_lat = getNormalizedLatitude(obj.getLatitude());
        long norm_height = 0;
        //double tep3 = 0;
        if (dimensions == 3) {
            norm_height = getNormalizedHeight(obj.getHeight());
            // tep3 = (xyz[2] - this.universeLowCorner[2])/this.lengthDimReal(2);

        }

        //double tep1 = (xyz[0] - this.universeLowCorner[0])/this.lengthDimReal(0);
        //double tep2 = (xyz[1] - this.universeLowCorner[1])/this.lengthDimReal(1);

        //System.out.println(this.lengthDimReal(0)+" "+this.lengthDimReal(1)+"for point with lon and lat "+lon+","+lat+" has normalized long and lat "+norm_lon+","+norm_lat+" "+tep1+" "+tep2);

        int x_cell = (int) norm_lat;
        int y_cell = (int) norm_lon;
        int z_cell = (int) norm_height;
        if (x_cell > this.sizex - 1) x_cell = this.sizex - 1;
        if (y_cell > this.sizey - 1) y_cell = this.sizey - 1;
        if (z_cell > this.sizez - 1) z_cell = this.sizez - 1;
        int[] return_array = new int[]{x_cell, y_cell, z_cell};
        return (return_array);
    }


    public double[][] getCellValues(int cell[]) {
        double[][] coord = new double[2][2];
        double li = (this.universeUpperCorner[0] - this.universeLowCorner[0]) / (double) this.sizex;
        double lj = (this.universeUpperCorner[1] - this.universeLowCorner[1]) / (double) this.sizey;
        double lz = 0;
        if (dimensions == 3)
            lz = (this.universeUpperCorner[2] - this.universeLowCorner[2]) / (double) this.sizez;


        //lower (long lat)
        coord[0][0] = this.universeLowCorner[0] + cell[0] * li;
        coord[0][1] = this.universeLowCorner[1] + cell[1] * lj;
        //upper
        coord[1][0] = this.universeLowCorner[0] + (cell[0] + 1) * li;
        coord[1][1] = this.universeLowCorner[1] + (cell[1] + 1) * lj;

        if (dimensions == 3) {
            coord[0][2] = this.universeLowCorner[2] + cell[2] * lz;
            coord[1][2] = this.universeLowCorner[2] + (cell[2] + 1) * lz;

        }

        //System.out.println(celli + " "+cellj);
        //System.out.println(coord[0][0] + " "+coord[0][1]);
        //System.out.println(coord[1][0] + " "+coord[1][1]);
        return coord;
    }

    public int getNumberOfbits() {
        return numberOfbits;
    }

    public double[] getUniverseLowCorner() {
        return universeLowCorner;
    }

    public double[] getUniverseUpperCorner() {
        return universeUpperCorner;
    }

    public int[] getCountersArray() {
        return countersArray;
    }

    @Override
    public String toString() {

        return "Grid [countersArray=" + Arrays.toString(countersArray) + "]";
    }

}
