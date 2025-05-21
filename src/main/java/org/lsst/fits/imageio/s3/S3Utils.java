package org.lsst.fits.imageio.s3;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import io.minio.GetObjectResponse;
import io.minio.MinioAsyncClient;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.logging.Logger;
import nom.tam.util.FitsFile;

/**
 *
 * @author tonyj
 */
public class S3Utils {

    private static final Logger LOG = Logger.getLogger(S3Utils.class.getName());

    private final static LoadingCache<String, RandomAccessObject> raoCache = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .removalListener((String file, RandomAccessObject rao, RemovalCause cause) -> rao.close())
            .build((String k) -> createRandomAccessObjectFor(k));

    private final static LoadingCache<String, AsynchronousFileChannel> fileCache = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(1))
            .removalListener((String file, AsynchronousFileChannel af, RemovalCause cause) -> closeAsynchronousFile(af))
            .build((String k) -> createAsynchronousFileFor(k));

    public static FitsFile openFile(String file) throws IOException {
        if (file.startsWith("s3:")) {
            RandomAccessObject rao = getRandomAccessObjectFor(file);
            return new FitsFile(rao, FitsFile.DEFAULT_BUFFER_SIZE);
        } else {
            return new FitsFile(file, "r");
        }
    }

    private static RandomAccessObject getRandomAccessObjectFor(String file) {
        return raoCache.get(file);
    }

    private static RandomAccessObject createRandomAccessObjectFor(String file) throws IOException {
        String s3File = file.substring(3);
        String[] clientBucketAndObject = s3File.split("/", 3);
        // We rely on an env var being set for accessing the bucket, using MINIO conventions
        String env = System.getenv("MC_HOST_" + clientBucketAndObject[0]);
        if (env == null) {
            throw new IOException("Missing definition for bucket " + clientBucketAndObject[0]);
        } else {
            // TODO: More resilient parsing
            URL url = new URL(env);
            String[] userInfoArray = url.getUserInfo().split(":");
            MinioAsyncClient minioClient
                    = MinioAsyncClient.builder()
                            .endpoint(url.getProtocol() + "://" + url.getHost())
                            .credentials(userInfoArray[0], userInfoArray[1])
                            .build();
            return new RandomAccessObject(minioClient, clientBucketAndObject[1], clientBucketAndObject[2]);
        }

    }

    public static CompletableFuture<ByteBuffer> readByteBufferAsync(String file, long seekPosition, int rawDataLength, long fileSize) {
        if (file.startsWith("s3:")) {
            return readByteBufferAsyncFromS3(file, seekPosition, rawDataLength);
        } else {
            return readByteBufferAsyncFromFile(file, seekPosition, rawDataLength);
        }

    }

    private static CompletableFuture<ByteBuffer> readByteBufferAsyncFromS3(String file, long seekPosition, int rawDataLength) {
        try {
            RandomAccessObject rao = getRandomAccessObjectFor(file);
            CompletableFuture<GetObjectResponse> asyncRead = rao.asyncRead(seekPosition, rawDataLength);
            return asyncRead.thenApplyAsync(a -> {
                try (a) {
                    byte[] buffer = new byte[rawDataLength];
                    int readNBytes = a.readNBytes(buffer, 0, buffer.length);
                    ByteBuffer bb = ByteBuffer.wrap(buffer, 0, readNBytes);
                    bb.order(ByteOrder.BIG_ENDIAN);
                    return bb;
                } catch (IOException x) {
                    throw new CompletionException("Error reading S3 file: " + file, x);
                }
            });
        } catch (InsufficientDataException | InternalException | InvalidKeyException | IOException | NoSuchAlgorithmException | XmlParserException ex) {
            return CompletableFuture.failedFuture(ex);
        }
    }

    private static CompletableFuture<ByteBuffer> readByteBufferAsyncFromFile(String file, long seekPosition, int rawDataLength) {
        CompletableFuture<ByteBuffer> result = new CompletableFuture<>();       
        AsynchronousFileChannel asyncChannel = fileCache.get(file);
        ByteBuffer bb = ByteBuffer.allocateDirect(rawDataLength);
        bb.order(ByteOrder.BIG_ENDIAN);
        asyncChannel.read(bb, seekPosition, result, new CompletionHandler<Integer, CompletableFuture<ByteBuffer>>() {
            @Override
            public void completed(Integer len, CompletableFuture<ByteBuffer> future) {
                bb.flip();
                future.complete(bb);
            }

            @Override
            public void failed(Throwable x, CompletableFuture<ByteBuffer> future) {
                future.completeExceptionally(x);
            }
        });
        return result;
    }

    private static AsynchronousFileChannel createAsynchronousFileFor(String file) throws IOException {
        return AsynchronousFileChannel.open(Path.of(file), StandardOpenOption.READ);
    }

    private static void closeAsynchronousFile(AsynchronousFileChannel af) {
        try {
            af.close();
        } catch (IOException x) {
            // Ignored.
        }
    }
}
