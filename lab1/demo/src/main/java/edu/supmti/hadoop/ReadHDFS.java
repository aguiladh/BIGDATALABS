import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path nomcomplet = new Path("/user/root/purchases.txt"); // Toujours codé en dur
        FSDataInputStream inStream = fs.open(nomcomplet);
        InputStreamReader isr = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        
        // Lit le fichier ligne par ligne
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        
        // L'instruction System.out.println(line); qui était ici a été supprimée.
        
        inStream.close();
        fs.close();
    }
}