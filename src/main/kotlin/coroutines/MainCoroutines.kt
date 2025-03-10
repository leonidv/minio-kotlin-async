package lv.coroutines

import io.minio.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.ByteArrayInputStream
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

const val BUCKET_NAME = "async-test"

fun main() {
   val time = measureTimeMillis {
        runBlocking {
            val client =
                MinioAsyncClient
                    .builder()
                    .endpoint("http://localhost:9000")
                    .credentials("PLDPXUML2fLdl7mNXwt6", "GPtMGhPaheOtoTqbmtDsdFgPOlvaKZGTCZuWZjRn")
                    .build();

            makeBucket(BUCKET_NAME, client)

            (1..10_000).forEach { key ->
                launch(Dispatchers.Default) {  putObject(BUCKET_NAME, "$key", client) }
            }
        }
       println("All works done!")
    }

    println("total ms: $time")
    exitProcess(1)
}

suspend fun makeBucket(bucketName : String, mc : MinioAsyncClient) {
    val bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build()
    val makeBucketArgs = MakeBucketArgs.builder().bucket(bucketName).build()

    val isBucketExists =   mc.bucketExists(bucketExistsArgs).await();
    if (!isBucketExists) {
        mc.makeBucket(makeBucketArgs).await()
    }
}


suspend fun putObject(bucketName: String, key : String, client: MinioAsyncClient) {
    generateBlob().use { binary ->
        println("${Thread.currentThread().name} $key has size ${binary.available()}")

        val putObjectArgs = PutObjectArgs.builder()
            .bucket(bucketName)
            .`object`(key)
            .stream(binary, binary.available().toLong(), -1)
            .build()
        client.putObject(putObjectArgs).await()

        println("${Thread.currentThread().name} - $key")
    }
}

/**
 * Generate random blob with size from 5Kb to 100Kb
 */
fun generateBlob() : ByteArrayInputStream {
    val size = Random.nextInt(5*1024,100*1024)
    return Random.nextBytes(size).inputStream()
};
