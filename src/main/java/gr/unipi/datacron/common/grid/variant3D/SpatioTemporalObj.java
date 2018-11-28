package gr.unipi.datacron.common.grid.variant3D;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SpatioTemporalObj {

    private double Latitude;
    private double Longitude;
    private double Height;
    private long Timestamp;


    public SpatioTemporalObj(double Longitude, double Latitude, double Height, long Timestamp) {
        this.Latitude = Latitude;
        this.Longitude = Longitude;
        this.Height = Height;
        this.Timestamp = Timestamp;
    }


    public SpatioTemporalObj() {
        this.Latitude = 0;
        this.Longitude = 0;
        this.Height = 0;
        this.Timestamp = -1;
    }

    public void setTime(String value) {
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss z");
            Date date = dateFormat.parse(value);
            this.Timestamp = date.getTime();
            //System.out.println(date.getTime()+"  "+value+" "+date.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
    
   /* public void setLongLat(String value)
    {
    	int i = value.indexOf("(")+1;
    	String t = value.substring(i);
    	i = t.indexOf(" ");
    	double lon = Double.parseDouble(t.substring(0, i));
    	//System.out.println(lat+" "+t.substring(0, i)+" "+value);
    	int j = t.indexOf(")");
    	double lat = Double.parseDouble(t.substring(i+1,  j));
    	//System.out.println(lon+" "+t.substring(i+1, t.length()-1)+" "+value);
    	this.Longitude = lon;
    	this.Latitude =  lat;
    }*/

    public String toString() {
        return "[ " + Longitude + "," + Latitude + "," + Timestamp + "]";
    }

    public double getLatitude() {
        return Latitude;
    }

    public void setLatitude(double latitude) {
        Latitude = latitude;
    }

    public double getLongitude() {
        return Longitude;
    }

    public void setLongitude(double longitude) {
        Longitude = longitude;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public double getHeight() {
        return Height;
    }


    public void setHeight(double height) {
        Height = height;
    }


    public void setTimeInterval(long timestamp) {
        Timestamp = timestamp;
    }
}
