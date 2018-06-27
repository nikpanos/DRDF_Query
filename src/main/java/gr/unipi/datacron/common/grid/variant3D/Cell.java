package gr.unipi.datacron.common.grid.variant3D;

public class Cell {

    public  Long x;
    public  Long y;
    public  Long z;

    public Cell(Long x, Long y, Long z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }
    public String toString() {
		return "["+ x + ", " +y+", " +z+"]";
	}
	public Long getX() {
		return x;
	}
	public Long getY() {
		return y;
	}
	public Long getZ() {
		return z;
	}
}
