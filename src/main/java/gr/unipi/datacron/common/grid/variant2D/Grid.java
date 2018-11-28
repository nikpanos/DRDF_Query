package gr.unipi.datacron.common.grid.variant2D;

public class Grid {

    //x==lat and y==long
    protected int numberOfbits;
    protected int sizex;
    protected int sizey;
    //Elaxistes times lat kai long
    protected double[] universeLowCorner;
    //Megistes times lat kai long
    protected double[] universeUpperCorner;

    //optional keeps the total number of points
    protected int[] countersArray;

    public Grid() {
    }

    public Grid(int numberOfbits, double[] universeLowCorner, double[] universeUpperCorner) {

        this.numberOfbits = numberOfbits;
        this.universeLowCorner = universeLowCorner;
        this.universeUpperCorner = universeUpperCorner;


    }


    public Cell hilbert2cell(long value) {
        HilbertCurve2D chc = new HilbertCurve2D((int) this.numberOfbits / 2, (int) this.numberOfbits / 2);
        Cell res = chc.toPoint(value);
        return res;
    }

    public long cell2hilbert(int a, int b) {
        HilbertCurve2D chc = new HilbertCurve2D((int) this.numberOfbits / 2, (int) this.numberOfbits / 2);
        return chc.toIndex(a, b);
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
        for (int i = 0; i < countersArray.length; i++) {
            countersArray[i] = countersArray[i] + counters[i];
        }
    }

    //returns the coordinates ij (integers row and column) of the cell
    public int[] getCell(Double lat, Double lon) {
        long norm_lon = getNormalizedLongitude(lon);
        long norm_lat = getNormalizedLatitude(lat);
        //System.out.println(this.lengthDimReal(0)+" "+this.lengthDimReal(1)+"for point with lon and lat "+lon+","+lat+" has normalized long and lat "+norm_lon+","+norm_lat);

        int x_cell = (int) norm_lat;
        int y_cell = (int) norm_lon;
        if (x_cell > this.sizex - 1) x_cell = this.sizex - 1;
        if (y_cell > this.sizey - 1) y_cell = this.sizey - 1;
        int[] return_array = new int[]{x_cell, y_cell};
        return (return_array);
    }


    public double[][] getCellValues(int celli, int cellj) {
        double[][] coord = new double[2][2];
        double li = (this.universeUpperCorner[0] - this.universeLowCorner[0]) / (double) this.sizex;
        double lj = (this.universeUpperCorner[1] - this.universeLowCorner[1]) / (double) this.sizey;

        //lower (long lat)
        coord[0][0] = this.universeLowCorner[0] + celli * li;
        coord[0][1] = this.universeLowCorner[1] + cellj * lj;
        //upper
        coord[1][0] = this.universeLowCorner[0] + (celli + 1) * li;
        coord[1][1] = this.universeLowCorner[1] + (cellj + 1) * lj;

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

}

