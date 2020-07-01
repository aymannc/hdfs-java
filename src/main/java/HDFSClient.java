import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Random;

public class HDFSClient {
    final String APP_USER = "/user_data/";
    final String FILE_NAME = "TechCrunchcontinentalUSA.csv";
    final String URL = "http://samplecsvs.s3.amazonaws.com/";
    Configuration configuration;
    FileSystem fileSystem;

    public HDFSClient(boolean deleteOldFiles) throws IOException {
        this.configuration = new Configuration();
        configuration.set("dfs.block.size", String.valueOf(2 * 1024 * 1024));
        System.setProperty("hadoop.home.dir", "c:\\hadoop\\");
        this.fileSystem = FileSystem.get(configuration);
        if (deleteOldFiles)
            fileSystem.delete(new Path(APP_USER), true);
        System.out.println("[INFO] Creating folder \npath: " + APP_USER);
        fileSystem.mkdirs(new Path(APP_USER));
    }


    public static void main(String[] args) throws IOException {
        HDFSClient hdfsClient = new HDFSClient(false);
        hdfsClient.createCSV();
        hdfsClient.downloadFile();
        hdfsClient.showCopyFromLocalFileResults();

    }

    private void createCSV() {

        System.out.println("[INFO] Creating the csv file");
        String filePath = APP_USER + "students.csv";
        System.out.println("file path: " + filePath);
        Path path = new Path(filePath);
        try {
            FSDataOutputStream onstream = fileSystem.create(path);
            onstream.writeBytes("NAME,GRADE");
            String[] names = {"a", "b", "c", "d"};
            for (String name : names) {
                onstream.writeBytes("\n" + name + ',' + (new Random().nextInt(21)));
            }
            onstream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void downloadFile() throws IOException {
        System.out.println("[INFO] Downloading the file");
        URL fileURL = new URL(URL + FILE_NAME);
        String filepath = System.getProperty("user.home") + "/output/files/" + FILE_NAME;
        System.out.println("Local file path: " + filepath);
        File outFile = new File(filepath);
        FileUtils.copyURLToFile(
                fileURL,
                outFile,
                100000, 100000
        );
        fileSystem.copyFromLocalFile(new Path(filepath), new Path(APP_USER));
    }

    private void getAllFilePath(Path filePath) throws IOException {
        FileStatus[] fileStatus = fileSystem.listStatus(filePath);
        for (FileStatus status : fileStatus) {
            if (status.isDirectory()) {
                getAllFilePath(status.getPath());
            } else {
                System.out.printf("%-12s%-15s%-10d%-30s%-25s\n",
                        status.getPermission().toString(),
                        status.getOwner(),
                        status.getLen(),
                        new Date(status.getModificationTime()).toString(),
                        status.getPath().toString()
                );
            }
        }
    }

    private void showCopyFromLocalFileResults() {
        System.out.println("[INFO] show CopyFromLocalFile results");
        System.out.printf("%-12s%-15s%-10s%-30s%-25s\n",
                "Permission",
                "Owner",
                "Len",
                "Modify Date",
                "Path"
        );
        try {
            this.getAllFilePath(new Path(this.APP_USER));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
