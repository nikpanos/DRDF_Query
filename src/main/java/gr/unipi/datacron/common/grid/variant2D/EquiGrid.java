package gr.unipi.datacron.common.grid.variant2D;

import java.util.HashMap;
import scala.Tuple2;

public class EquiGrid extends Grid{


	//optional keeps the total number of points
	//private int [] countersArray;

	public EquiGrid()
	{	
	}

	public EquiGrid(int numberOfbits , double[] universeLowCorner, double[] universeUpperCorner) {
		super( numberOfbits , universeLowCorner, universeUpperCorner);

		this.sizex = (int)Math.pow(2, (int) numberOfbits/2);
		this.sizey = (int)Math.pow(2, (int) numberOfbits/2);
	}
	
	public HashMap<Long,Boolean> getGridCells(Tuple2<Double, Double> loCorner, Tuple2<Double, Double> hiCorner)
	{
		HashMap <Long,Boolean> cells = new HashMap <Long,Boolean>();
		int spatialstart[] = this.getCell(loCorner._1, loCorner._2);
		int spatialend[] = this.getCell(hiCorner._1, hiCorner._2);
		for (int i=spatialstart[0];i<=spatialend[0];i++)
		{

			//hilbert value i, spatialstart[1]
			long h = this.cell2hilbert(i,spatialstart[1]);
			cells.put(h, true);
			//hilbert value i, spatialend[1]
			h = this.cell2hilbert(i,spatialend[1]);
			cells.put(h, true);	
		}
		for (int j=spatialstart[1]+1;j<spatialend[1];j++)
		{
			//hilbert value spatialstart[0], i, 
			long h = this.cell2hilbert(spatialstart[0], j);
			cells.put(h, true);
			//hilbert value spatialend[0],i,
			h = this.cell2hilbert(spatialend[0], j);
			cells.put(h, true);
		}
		for (int i=spatialstart[0]+1;i<spatialend[0];i++)
		{
			for (int j=spatialstart[1]+1;j<spatialend[1];j++)
			{
				//hilbert value i,j, 	
				long h = this.cell2hilbert(i, j);
				cells.put(h, false);
				//System.out.println("cell without refined spatial "+h);
			}
		}
		//System.out.println(cells.keySet());
		return cells;

	}

}
