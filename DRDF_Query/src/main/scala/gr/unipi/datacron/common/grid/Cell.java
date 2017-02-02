package gr.unipi.datacron.common.grid;

public class Cell {

    public  Long x;
    public  Long y;
    

    public Cell(Long x, Long y) {
        this.x = x;
        this.y = y;
    }
    public String toString() {
		return "["+ x + ", " +y+"]";
	}
	public Long getX() {
		return x;
	}
	public Long getY() {
		return y;
	}

}
