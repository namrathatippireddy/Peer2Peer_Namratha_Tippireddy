
import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

public class Utils {

    /**
     * Method to split the file and saves all the chunks in a folder with in the parents folder of
     * original file
     */
    public static int splitFile(File inputFile) throws IOException {
        int chunkCounter = 1;

        int sizeOfChunk = 100 * 1024;// 100KB
        byte[] buffer = new byte[sizeOfChunk];

        String fileName = inputFile.getName();

        try (FileInputStream fis = new FileInputStream(inputFile);
             BufferedInputStream bis = new BufferedInputStream(fis)) {

            int bytesAmount = 0;
            while ((bytesAmount = bis.read(buffer)) > 0) {
                //write each chunk of data into separate file with different number in name
                String chunkName = Integer.toString(chunkCounter);
                File newFile = new File(inputFile.getParent()+"/chunks/", chunkName);
                try (FileOutputStream out = new FileOutputStream(newFile)) {
                    out.write(buffer, 0, bytesAmount);
                }
                chunkCounter++;
            }
        }
        return chunkCounter-1;
    }

}