package gr.unipi.datacron.common.grid.variant3D;

public class ZorderCurve extends EncodingCurve{

	public ZorderCurve(int bits, int dimensions){
		super(bits,dimensions);	   
	}


	public long toIndex(int x[])
	{
		String coor[] = new String[x.length];
		for (int i=0;i<x.length;i++)
		{
			coor[i]=Long.toBinaryString(x[i]);
		}
		int max = coor[0].length();
		for (int i=1;i<coor.length;i++)
		{
			if (max<coor[i].length()) max = coor[i].length();
		}
		String out = new String();
		int cnt = max-1;
		while (cnt>=0)
		{
			for (int i=0;i<coor.length;i++)
			{
				if (cnt>=coor[i].length()) out=out+"0";
				else 
					out=out+coor[i].charAt(coor[i].length()-1-cnt);
			}
			cnt--;
		}
		Long l = Long.parseLong(out, 2);
		return (l);
	}

	public Cell toPoint(long value1)
	{
		String c = Long.toBinaryString(value1);
		int len = c.length();
		int y = len%dimensions;

		String co[]=new String[dimensions];
		for (int j=0;j<co.length;j++)
		{
			co[j]=new String();
		}
		if (y!=0)
		{
			String temp="";
			for (int j=0;j<dimensions-y;j++)
			{
				temp=temp+"0";
			}
			c = temp+c;
		}
		int i=0;
		while(i<c.length())
		{
			for (int j=0;j<co.length;j++)
			{
				co[j]=co[j]+c.charAt(i);
				i++;
			}
		}
		//System.out.println(co[0]+" "+co[1]+" "+co[2]+" ");
		Cell t ;
		if (dimensions==2)  t = new Cell(Long.parseLong(co[0]), Long.parseLong(co[1]),0l);
		else
			 t = new Cell(Long.parseLong(co[0]), Long.parseLong(co[1]), Long.parseLong(co[2]));
		return(t);
	}


	public static void main(String[] args) {

		int b=4;
		ZorderCurve chc = new ZorderCurve(b,b) ;
		for (int j=0;j<b;j++)
		{
			for (int i=0;i<b;i++)
			{ 

				int l [] = new int[2];
				l[0]=j;
				l[1]=i;
				long hilbert_value = chc.toIndex(l);
				System.out.println(j+" "+i+" "+hilbert_value);

			}
		}

	}


}
