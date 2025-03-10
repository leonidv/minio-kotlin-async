package lv.cf

import io.minio.*
import java.io.ByteArrayInputStream
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.

const val BUCKET_NAME = "async-test"

fun main() {
    val client =
        MinioAsyncClient
            .builder()
            .endpoint("http://localhost:9000")
            .credentials("PLDPXUML2fLdl7mNXwt6", "GPtMGhPaheOtoTqbmtDsdFgPOlvaKZGTCZuWZjRn")
            .build();

    val time = measureTimeMillis {
        val cf = makeBucket(BUCKET_NAME, client)
        cf.thenRun {
            println("put objects")
            val putFeatures = (1..10_000).map { key ->
                putObject(BUCKET_NAME, "$key", client)
                    .handle { _, thr ->
                        thr?.printStackTrace()
                    }
            }
            CompletableFuture.allOf(*putFeatures.toTypedArray()).join()

        }
        cf.handle { _, thr -> thr?.printStackTrace() }
        cf.join();
        println("All works done!")
    }
    println("total ms: $time") // total ms: 80781
    exitProcess(1)
}

fun makeBucket(bucketName : String, mc : MinioAsyncClient) : CompletableFuture<Void?> {
    val bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build()
    val makeBucketArgs = MakeBucketArgs.builder().bucket(bucketName).build()

    val cf = mc
        .bucketExists(bucketExistsArgs)
        .thenAccept { bucketExists ->
            if (!bucketExists) {
                mc.makeBucket(makeBucketArgs)
            }
        }
    return cf;
}

fun putObject(bucketName: String, key : String, client: MinioAsyncClient): CompletableFuture<ObjectWriteResponse> {
    val binary = generateBlob()
    println("${Thread.currentThread().name} $key has size ${binary.available()}")
    val putObjectArgs = PutObjectArgs.builder()
        .bucket(bucketName)
        .`object`(key)
        .stream(binary,binary.available().toLong(),-1)
        .build()
    val cf = client.putObject(putObjectArgs)
    cf.handle { _, throwable ->
        println("${Thread.currentThread().name} - $key")
        binary.close()
    }
    return cf;
}

/**
 * Generate random blob with size from 5Kb to 100Kb
 */
fun generateBlob() : ByteArrayInputStream {
    val size = Random.nextInt(5*1024,100*1024)
    return Random.nextBytes(size).inputStream()
};
