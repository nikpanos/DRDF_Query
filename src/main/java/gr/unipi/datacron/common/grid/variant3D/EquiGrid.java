package gr.unipi.datacron.common.grid.variant3D;

import scala.Tuple2;

import java.util.HashMap;

public class EquiGrid extends Grid {

	public EquiGrid() {
	}

	public EquiGrid(int numberOfbits , double[] universeLowCorner, double[] universeUpperCorner, int dimensions, int curveType) {
		super( numberOfbits , universeLowCorner, universeUpperCorner,dimensions,curveType);

		this.sizex = (int)Math.pow(2, (int) numberOfbits/dimensions);
		this.sizey = (int)Math.pow(2, (int) numberOfbits/dimensions);
		this.sizez = (int)Math.pow(2, (int) numberOfbits/dimensions);
	}

    private boolean isOnBorder(int[] currentCell, int[] lowCorner, int[] highCorner) {
	    for (int i = 0; i < dimensions; i++) {
	        if ((currentCell[i] == lowCorner[i]) || (currentCell[i] == highCorner[i])) {
	            return true;
            }
        }
        return false;
    }

    private void getGridCells(HashMap<Long,Boolean> cells, int d, int[] currentCell, int[] sLow, int[] sHigh, EncodingCurve chc) {
        for (int i = sLow[d]; i <= sHigh[d]; i++) {
            currentCell[d] = i;
            if ((d + 1) < dimensions) {
                getGridCells(cells, d + 1, currentCell, sLow, sHigh, chc);
            }
            else {
                long h = chc.toIndex(currentCell);
                cells.put(h, isOnBorder(currentCell, sLow, sHigh));
            }
        }
    }

    private int[] getZeroCellOfSize(int size) {
	    int[] result = new int[size];
	    for (int i = 0; i < size; i++) {
	        result[i] = 0;
        }
        return result;
    }

	public HashMap<Long,Boolean> getGridCells(SpatioTemporalObj loCorner, SpatioTemporalObj hiCorner)
	{
        int sLow[] = this.getCell(loCorner);
        int sHigh[] = this.getCell(hiCorner);

        EncodingCurve chc;
        if (this.curveType!=2) {
            chc = new HilbertCurve(numberOfbits, dimensions);
        }
        else {
            chc = new ZorderCurve(numberOfbits, dimensions);
        }
        HashMap<Long,Boolean> cells = new HashMap<>();
        getGridCells(cells, 0, getZeroCellOfSize(dimensions), sLow, sHigh, chc);
		return cells;

	}
}
