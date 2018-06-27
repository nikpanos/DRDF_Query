package gr.unipi.datacron.common.grid.variant3D;

public class EquiGrid extends Grid {


	//optional keeps the total number of points
	//private int [] countersArray;

	public EquiGrid()
	{	
	}

	public EquiGrid(int numberOfbits , double[] universeLowCorner, double[] universeUpperCorner, int dimensions, int curveType) {
		super( numberOfbits , universeLowCorner, universeUpperCorner,dimensions,curveType);

		this.sizex = (int)Math.pow(2, (int) numberOfbits/dimensions);
		this.sizey = (int)Math.pow(2, (int) numberOfbits/dimensions);
		this.sizez = (int)Math.pow(2, (int) numberOfbits/dimensions);
	}

}
