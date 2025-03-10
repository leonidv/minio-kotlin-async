package lv.coroutines

import io.minio.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.future.await
import java.io.ByteArrayInputStream
import java.util.concurrent.CompletableFuture
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

const val BUCKET_NAME = "async-test"

const val DEBUG = false;

fun main() {
   val time = measureTimeMillis {
        runBlocking {
            val client =
                MinioAsyncClient
                    .builder()
                    .endpoint("http://localhost:9000")
                    .credentials("PLDPXUML2fLdl7mNXwt6", "GPtMGhPaheOtoTqbmtDsdFgPOlvaKZGTCZuWZjRn")
                    //.credentials("F4bRMwM7cr0ScMVVRnA9","SGi919Bvtz1SyCOirzjeSOm6JGRA1tiWemqJY4qb")
                    .build();

             makeBucket(BUCKET_NAME, client)

            (1..100_000).forEach { key ->
                launch(Dispatchers.IO) {  putObject(BUCKET_NAME, "$key", client) }
            }
        }
       println("All works done!")
    }

    println("total ms: $time")
    exitProcess(1)
}

fun debug(msg : String) {
    if (DEBUG) {
        println(msg)
    }
}

suspend fun makeBucket(bucketName : String, mc : MinioAsyncClient) {
    val bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build()
    val makeBucketArgs = MakeBucketArgs.builder().bucket(bucketName).build()
    debug("${Thread.currentThread().name} check bucket exists")
    val isBucketExists =   mc.bucketExists(bucketExistsArgs).await();
    if (!isBucketExists) {
        mc.makeBucket(makeBucketArgs).await()
    }
}


suspend fun putObject(bucketName: String, key : String, client: MinioAsyncClient) =
     coroutineScope {
         val blob = async { generateBlob(key) }.await()
         blob.use { binary ->
             debug("${Thread.currentThread().name} $key has size ${binary.available()}")

             val putObjectArgs = PutObjectArgs.builder()
                 .bucket(bucketName)
                 .`object`(key)
                 .stream(binary, binary.available().toLong(), -1)
                 .build()
             client.putObject(putObjectArgs).await()

             debug("${Thread.currentThread().name} - $key")
         }
    }

/**
 * Generate random blob with size from 5Kb to 100Kb
 */
suspend fun generateBlob(taskId : String) : ByteArrayInputStream {
    return coroutineScope {
        debug("${Thread.currentThread().name} $taskId begin generate blob")
        val size = async {
            debug("${Thread.currentThread().name} $taskId generate blob's size")
            Random.nextInt(5*1024,50*1024)
        }.await()
        async {
            debug("${Thread.currentThread().name} $taskId generate blob")
            Random.nextBytes(size).inputStream()
        }.await()
    }
};
