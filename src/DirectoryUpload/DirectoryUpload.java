package DirectoryUpload;

import java.net.URL;
import java.util.Random;
import java.util.stream.Stream;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.Paths;

import com.microsoft.azure.storage.blob.*;
import com.microsoft.azure.storage.blob.models.StorageErrorCode;
import com.microsoft.rest.v2.RestException;

import io.reactivex.*;
import io.reactivex.functions.Function;

public class DirectoryUpload {
    public static void main(String[] args) throws java.lang.Exception {

        // Retrieve the credentials and initialize SharedKeyCredentials from the system env variables
        String accountName = System.getenv("AZURE_STORAGE_ACCOUNT");
        String accountKey = System.getenv("AZURE_STORAGE_ACCESS_KEY");
        SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);

        // Directory to upload
        Path filePath = Paths.get("C:\\path\\to\\directory");

        // Create a ServiceURL to call the Blob service. We will also use this to construct the ContainerURL
        // Alternatively you can create the BlockBlobURL object directly
        // We are using a default pipeline here, you can learn more about it at https://github.com/Azure/azure-storage-java/wiki/Azure-Storage-Java-V10-Overview
        final ServiceURL serviceURL = new ServiceURL(new URL("http://" + accountName + ".blob.core.windows.net"), StorageURL.createPipeline(creds, new PipelineOptions()));

        // Walk the directory and filter for .xml files
        Stream<Path> walk = Files.walk(filePath).filter(p -> p.toString().endsWith(".xml"));
        // .filter(Files::isRegularFile) to upload all files ignoring directories

        // Construct an observable which will upload files found asynchronously into Blob storage in 20 concurrent operations
        Observable<CommonRestResponse> uploadUpservable = Observable.fromIterable(() -> walk.iterator())
                .flatMap(path -> {
            BlockBlobURL blobURL = containerURL.createBlockBlobURL(path.getFileName().toString());

            //FileChannel fc = FileChannel.open(path);
            return Single.using(() -> FileChannel.open(),
                    fc -> {
                        return TransferManager.uploadFileToBlockBlob(
                                fc, blobURL,
                                BlockBlobURL.MAX_PUT_BLOCK_BYTES, null).toObservable()
                                .doOnError(throwable -> {
                                    if (throwable instanceof RestException) {
                                        System.out.println("Failed to upload " + path + " with error:" +
                                                ((RestException) throwable).response().statusCode());
                                    } else {
                                        System.out.println(throwable.getMessage());
                                    }
                                })
                                .doAfterTerminate(() -> {
                                    System.out.println("Upload of " + path + " completed");
                                });
                    },
                    FileChannel::close);

        }, 20);  // Max concurrency of 20

        // Let's create a container and map the response to the Observable we created above.
        // If container exists, we'll catch and continue with the same Observable.
        Random rand = new Random();
        ContainerURL containerURL = serviceURL.createContainerURL("quickstart" + rand.nextInt(1000));
        System.out.println("Creating a container at " + containerURL.toString());
        containerURL.create(null, null)
        .toObservable().flatMap(response -> uploadUpservable)
        .onErrorResumeNext(throwable -> {
            if (throwable instanceof StorageException
            ((StorageException) throwable).errorCode() == StorageErrorCode.CONTAINER_ALREADY_EXISTS) {
                return uploadUpservable;
            }
            return Single.error(throwable)
        })
        .subscribe();

        // To block the main via user input
        System.in.read();
    }
}
