package lv.coroutines

import io.minio.*
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import java.io.ByteArrayInputStream
import kotlin.math.max
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis

const val BUCKET_NAME = "async-test"

const val DEBUG = true;

const val OBJECTS_COUNT = 10_000

val s3dispatcher = Dispatchers.IO.limitedParallelism(max(OBJECTS_COUNT,16),"s3-limited")
val cpuIntensiveDispatcher = Dispatchers.IO.limitedParallelism(2,"cpu intensive")
fun main() {
    val client =
        MinioAsyncClient
            .builder()
            .endpoint("http://localhost:9000")
            .credentials("PLDPXUML2fLdl7mNXwt6", "GPtMGhPaheOtoTqbmtDsdFgPOlvaKZGTCZuWZjRn")
            //.credentials("F4bRMwM7cr0ScMVVRnA9","SGi919Bvtz1SyCOirzjeSOm6JGRA1tiWemqJY4qb")
            .build();



    val putTime = measureTimeMillis {
        runBlocking {
            makeBucket(BUCKET_NAME, client)

            (1..OBJECTS_COUNT).forEach { key ->
                launch(s3dispatcher) { putObject(BUCKET_NAME, "$key", client) }
            }
        }
    }

    println("put $OBJECTS_COUNT objects in ${putTime}ms")
    println("=".repeat(80))

    var downloadedSize = -1;
    val getTime = measureTimeMillis {
        downloadedSize = runBlocking {
            (1..OBJECTS_COUNT).map { key ->
                val response = async(s3dispatcher) {
                    loadBlob("$key",client)
                }.await()
                val obj = async (s3dispatcher) {
                    response.readAllBytes()
                }.await()
                obj.size
            }.reduce{ acc, sum -> acc + sum}
        }
    }

    println("get $OBJECTS_COUNT objects, total size: $downloadedSize in $getTime")
    exitProcess(1)
}

fun debug(taskId : String, msg: String = "") {
    if (DEBUG) {
        println("${Thread.currentThread().name} - $taskId $msg")
    }
}

suspend fun makeBucket(bucketName: String, mc: MinioAsyncClient) {
    val bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build()
    val makeBucketArgs = MakeBucketArgs.builder().bucket(bucketName).build()
    debug("", "check bucket exists")
    val isBucketExists = mc.bucketExists(bucketExistsArgs).await();
    if (!isBucketExists) {
        mc.makeBucket(makeBucketArgs).await()
    }
}


suspend fun putObject(bucketName: String, taskId: String, client: MinioAsyncClient) =
    coroutineScope {
        val blob = async(s3dispatcher) { generateBlob(taskId) }.await()
        blob.use { binary ->
            debug(taskId, "has size ${binary.available()}")

            val putObjectArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .`object`(taskId)
                .stream(binary, binary.available().toLong(), -1)
                .build()
            client.putObject(putObjectArgs).await()

            debug(taskId,"object put to S3")
        }
    }

/**
 * Generate random blob with size from 5Kb to 100Kb
 */
fun generateBlob(taskId: String): ByteArrayInputStream {
    debug(taskId, "generate blob's size")
    val size = Random.nextInt(5 * 1024, 50 * 1024)
    return Random.nextBytes(size).inputStream()
};

suspend fun loadBlob(taskId: String, client: MinioAsyncClient): GetObjectResponse {
    //val downloadArgs = DownloadObjectArgs.builder().bucket(BUCKET_NAME).
    val getArgs = GetObjectArgs.builder().bucket(BUCKET_NAME).`object`(taskId).build();
    debug(taskId, "Before get object")
    val cf = client.getObject(getArgs);
    val objectResponse = cf.await();
    debug(taskId, "After get response")
    return client.getObject(getArgs).await()
}