package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        
        
        if (args.length != 3) {
            System.out.println("Usage: hadoop jar <jar_file> <path> <old_filename> <new_filename>");
            System.out.println("Example: hadoop jar HadoopFileStatus.jar /user/root/input purchases.txt achats.txt");
            return;
        }
        
        
        String folderPath = args[0];
        String oldFileName = args[1];
        String newFileName = args[2];
        
        Configuration conf = new Configuration();
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
            
           
            Path oldFilePath = new Path(folderPath, oldFileName); 
            Path newFilePath = new Path(folderPath, newFileName);

            
            if (!fs.exists(oldFilePath)) {
                System.out.println("File " + oldFilePath.toString() + " does not exist.");
                return;
            }
            
            
            FileStatus status = fs.getFileStatus(oldFilePath);
            
            System.out.println("--- File Information ---");
            System.out.println("File Name: " + status.getPath().getName());
            System.out.println("File Size: " + status.getLen() + " bytes"); 
            System.out.println("File owner: " + status.getOwner()); 
            System.out.println("File permission: " + status.getPermission()); 
            System.out.println("File Replication: " + status.getReplication()); 
            System.out.println("File Block Size: " + status.getBlockSize());

            
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            System.out.println("--- Block Locations ---");
            for (BlockLocation blockLocation : blockLocations) { 
                
                System.out.println("Block offset: " + blockLocation.getOffset()); 
                System.out.print("Block hosts: ");
                for (String host : blockLocation.getHosts()) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

          
            if (fs.rename(oldFilePath, newFilePath)) { 
                 System.out.println("--- Rename Status ---");
                 System.out.println("File renamed successfully from " + oldFilePath.getName() + " to " + newFilePath.getName());
            } else {
                 System.out.println("File rename failed.");
            }

        } catch (IOException e) { 
            e.printStackTrace(); 
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}