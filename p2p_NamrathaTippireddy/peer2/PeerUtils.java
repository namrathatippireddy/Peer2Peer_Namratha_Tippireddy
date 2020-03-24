
import java.util.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.IOException;

public class PeerUtils {

    /**
     * Method to merge all the chunk files in to a single file
     */
    public static void mergeFile(int numOfChunks, String fileName) {
        String chunksDir = "./file/chunks";
        File  mergedFile = new File("./file/"+fileName);
        FileOutputStream fos;
        FileInputStream fis;
        byte[] fileBytes;
        int bytesRead = 0;
        List<File> list = new ArrayList<>();
        for (int i = 1; i <= numOfChunks; i++) {
            list.add(new File(chunksDir + "/" + i));
        }
        try {
            fos = new FileOutputStream(mergedFile);
            for (File file : list) {
                fis = new FileInputStream(file);
                fileBytes = new byte[(int) file.length()];
                bytesRead = fis.read(fileBytes, 0, (int) file.length());
                fos.write(fileBytes);
                fos.flush();
                fileBytes = null;
                fis.close();
                fis = null;
            }
            fos.close();
            fos = null;
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}