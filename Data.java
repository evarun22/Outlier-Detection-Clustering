import java.io.*;
import java.util.*;

public class Data {
    public static void main(String args[]) throws IOException{
        File P = new File("P.csv");
        P.createNewFile();
        File R = new File("R.csv");
        R.createNewFile();
        FileWriter fwp = new FileWriter("P.csv");
        
        int numPoints = 10300000;
        for(int i=0;i<numPoints;i++){
            Random r = new Random();
            int xDimLow = 1;
            int xDimHigh = 10000;
            int xDim = r.nextInt(xDimHigh - xDimLow) + xDimLow;

            int yDimLow = 1;
            int yDimHigh = 10000;
            int yDim = r.nextInt(yDimHigh - yDimLow) + yDimLow;

            fwp.append(xDim+"");
            fwp.append(",");
            fwp.append(yDim+"");
            fwp.append("\n");
        }
        fwp.flush();
        fwp.close();

        FileWriter fwr = new FileWriter("R.csv");
        
        int numRectangles = 7000000;
        for(int i=0;i<numRectangles;i++){
            Random r = new Random();
            int xDimLow = 1;
            int xDimHigh = 9996;
            int xDim = r.nextInt(xDimHigh - xDimLow) + xDimLow;

            int yDimLow = 1;
            int yDimHigh = 9881;
            int yDim = r.nextInt(yDimHigh - yDimLow) + yDimLow;

            int heightLow = 1;
            int heightHigh = 21;
            int height = r.nextInt(heightHigh - heightLow) + heightLow;

            int widthLow = 1;
            int widthHigh = 6;
            int width = r.nextInt(widthHigh - widthLow) + widthLow;

            fwr.append(xDim+"");
            fwr.append(",");
            fwr.append(yDim+"");
            fwr.append(",");
            fwr.append(height+"");
            fwr.append(",");
            fwr.append(width+"");
            fwr.append("\n");
        }
        fwr.flush();
        fwr.close();
    }
}
