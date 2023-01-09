package org.lsst.fits.imageio.s3;

import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioAsyncClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.XmlParserException;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import nom.tam.util.RandomAccessFileIO;

/**
 * 
 * @author tonyj
 */
public class RandomAccessObject implements RandomAccessFileIO {

    private final String object;
    private final String bucket;
    private final MinioAsyncClient client;
    private long position;
    private long length;

    RandomAccessObject(MinioAsyncClient client, String bucket, String object) throws IOException {
        this.client = client;
        this.bucket = bucket;
        this.object = object;
        position = 0;
        try {
            CompletableFuture<StatObjectResponse> statObject = client.statObject(StatObjectArgs.builder()
                    .bucket(bucket)
                    .object(object)
                    .build()
            );
            this.length = statObject.get().size();
        } catch (InterruptedException ex) {
            throw new InterruptedIOException("Error reading bucket");
        } catch (ExecutionException ex) {
            throw new IOException("Error reading bucket", ex.getCause());
        } catch (InsufficientDataException | InternalException | InvalidKeyException | NoSuchAlgorithmException | XmlParserException ex) {
            throw new IOException("Error reading bucket", ex);
        }         
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public FileChannel getChannel() {
        return null;
    }

    @Override
    public FileDescriptor getFD() throws IOException {
        return null;
    }

    @Override
    public void setLength(long length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public void position(long n) throws IOException {
        System.out.printf("set position %d\n", n);        
        this.position = n;
    }

    @Override
    public long length() throws IOException {
        return length;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public int read(byte[] b, int from, int length) throws IOException {
        System.out.printf("read from %d length %d\n", position+from, length);
        try {
            CompletableFuture<GetObjectResponse> stream = client.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(object)
                            .offset((long) position)
                            .length((long) length)
                            .build()
                        );
            try (GetObjectResponse in = stream.get()) {
                final int readNBytes = in.readNBytes(b, from, length);
                position += readNBytes;
                return readNBytes;            
            }
        } catch (InsufficientDataException | InternalException | XmlParserException | IOException | IllegalArgumentException | InvalidKeyException | NoSuchAlgorithmException e) {
            throw new IOException("Error reading bucket", e);
        } catch (InterruptedException ex) {
            throw new InterruptedIOException("Error reading bucket");
        } catch (ExecutionException ex) {
            throw new IOException("Error reading bucket", ex.getCause());
        }
    }
    
    CompletableFuture<GetObjectResponse> asyncRead(long position, long length) throws InsufficientDataException, InternalException, InvalidKeyException, IOException, NoSuchAlgorithmException, XmlParserException {
        return client.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucket)
                            .object(object)
                            .offset(position)
                            .length(length)
                            .build()
                        );
    }
    

    @Override
    public void write(int b) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void write(byte[] b, int from, int length) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); // Generated from nbfs://nbhost/SystemFileSystem/Templates/Classes/Code/GeneratedMethodBody
    }

    @Override
    public void close() {
        // Nothing to do?
    }

}
