package com.example.storagedemo.controller;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Mono;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@RestController
@Slf4j
public class StorageController {
    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    @Autowired
    private Storage storage;

    @Value("java-demo")
    String bucketName;
    @Value("visionai")
    String subdirectory;

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<URL> uploadFile(@RequestPart("file") MultipartFile filePart) {
        //Convert the file to a byte array
        final byte[] byteArray = convertToByteArray(filePart);

        //Prepare the blobId
        //BlobId is a combination of bucketName + subdirectiory(optional) + fileName
        final BlobId blobId = constructBlobId(bucketName, subdirectory, filePart.getOriginalFilename());

        return Mono.just(blobId)
                //Create the blobInfo
                .map(bId -> BlobInfo.newBuilder(blobId)
                        .build())
                //Upload the blob to GCS
                .doOnNext(blobInfo -> getStorage().create(blobInfo, byteArray))
                //Create a Signed "Path Style" URL to access the newly created Blob
                //Set the URL expiry to 10 Minutes
                .map(blobInfo -> createSignedPathStyleUrl(blobInfo, 10, TimeUnit.MINUTES));
    }

    @GetMapping("/get-data")
    public String getData() throws IOException {
        StringBuffer sb = new StringBuffer();
        try (ReadChannel channel = storage.reader(bucketName, subdirectory + "/demofile.txt")) {
            ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
            while (channel.read(buffer) > 0) {
                buffer.flip();
                String data = new String(buffer.array(), 0, buffer.limit());
                sb.append(data);
                buffer.clear();
            }
            return sb.toString();
        }

    }

    @DeleteMapping("delete-data")
    public String deleteStorage(@RequestParam(name = "fileName") String fileName) {
        if (fileName == null) {
            return "file name not null";
        }
        BlobId idDelete = constructBlobId(bucketName, subdirectory, fileName);
        boolean isDelete = storage.delete(idDelete);
        if (!isDelete) {
            return "Delete failed";
        }
        return "Delete successly";
    }

    private URL createSignedPathStyleUrl(BlobInfo blobInfo, int duration, TimeUnit timeUnit) {
        return getStorage()
                .signUrl(blobInfo, duration, timeUnit, Storage.SignUrlOption.withPathStyle());
    }

    /**
     * Construct Blob ID
     *
     * @param bucketName
     * @param subdirectory optional
     * @param fileName
     * @return
     */
    private BlobId constructBlobId(String bucketName, @Nullable String subdirectory, String fileName) {
        return Optional.ofNullable(subdirectory)
                .map(s -> BlobId.of(bucketName, subdirectory + "/" + fileName))
                .orElse(BlobId.of(bucketName, fileName));
    }

    /**
     * Here, we convert the file to a byte array to be sent to GCS Libraries
     *
     * @param filePart File to be used
     * @return Byte Array with all the contents of the file
     */
    @SneakyThrows
    private byte[] convertToByteArray(MultipartFile filePart) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] bytes = filePart.getBytes();
        log.trace("readable byte count:" + bytes);

        try {
            bos.write(bytes);
        } catch (IOException e) {
            log.error("read request body error...", e);
        }
        return bos.toByteArray();
    }


}
