import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        
        // --- 1. Correction : Vérifier 1 seul argument ---
        if (args.length != 1) {
            System.err.println("Usage: hadoop jar WriteHDFS.jar <chemin_fichier_HDFS>");
            System.err.println("Exemple: hadoop jar /shared_volume/WriteHDFS.jar ./input/bonjour.txt");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        // Utilise l'argument 0 comme chemin 
        Path nomcomplet = new Path(args[0]); 
        
        if (!fs.exists(nomcomplet)) {
            FSDataOutputStream outStream = fs.create(nomcomplet); // [cite: 180]
            
            // Écrit la chaîne codée en dur 
            outStream.writeUTF("Bonjour tout le monde !"); 
            
            // --- 2. Correction : Ligne supprimée ---
            // La ligne outStream.writeUTF(args[1]);  est supprimée 
            // car l'exemple  ne fournit pas args[1].
            
            outStream.close(); // [cite: 182]
            System.out.println("Fichier créé : " + nomcomplet);
        } else {
            System.out.println("Le fichier " + nomcomplet + " existe déjà.");
        }
        
        fs.close(); // [cite: 184]
    }
}