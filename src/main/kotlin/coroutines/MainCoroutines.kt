package lv.coroutines

import io.minio.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.future.await
import okhttp3.OkHttpClient
import java.io.ByteArrayInputStream
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.math.max
import kotlin.random.Random
import kotlin.system.exitProcess
import kotlin.time.measureTime

const val BUCKET_NAME = "async-test"

const val DEBUG = true;

const val OBJECTS_COUNT = 100_000

val s3dispatcher = Dispatchers.IO.limitedParallelism(10, "s3-limited")
val cpuIntensiveDispatcher = Dispatchers.IO.limitedParallelism(2, "cpu intensive")
fun main() {
    Logger.getLogger(OkHttpClient::class.java.name).level = Level.FINE

    val client =
        MinioAsyncClient
            .builder()
            .endpoint("http://localhost:9000")
            .credentials("PLDPXUML2fLdl7mNXwt6", "GPtMGhPaheOtoTqbmtDsdFgPOlvaKZGTCZuWZjRn")
            //.credentials("F4bRMwM7cr0ScMVVRnA9","SGi919Bvtz1SyCOirzjeSOm6JGRA1tiWemqJY4qb")
            .build();


//    val putTime = measureTimeMillis {
//        runBlocking {
//            makeBucket(BUCKET_NAME, client)
//
//            (1..OBJECTS_COUNT).forEach { key ->
//                launch(s3dispatcher) { putObject(BUCKET_NAME, "$key", client) }
//            }
//        }
//    }
//
//    println("put $OBJECTS_COUNT objects in ${putTime}ms")
//    println("=".repeat(80))

    var downloadedSize = -1L;
    val getTime = measureTime {
        downloadedSize = runBlocking {
            var processed = 0;
            var sizeProcessed = 0L;
            val channelWithObjectSizes = Channel<Pair<String, Long>>(UNLIMITED)

            val percentOfTasks: Int = max(OBJECTS_COUNT / 100, 1);

            launch() {
                (1..OBJECTS_COUNT)
                    .map { it.toString() }
                    .map { key ->
                        launch(s3dispatcher) {
                            val response = async(s3dispatcher) {
                                loadBlob(key, client)
                            }.await();

                            response.use {
                                debug(key, "before readAllBytes()")
                                val blob =  response.readAllBytes()
                                debug(key, "after readAllBytes()")

                                val blobSize = blob.size.toLong()
                                channelWithObjectSizes.send(Pair(key, blobSize))
                                blobSize
                            }
                        }
                    }
            }.invokeOnCompletion { channelWithObjectSizes.close() }

            launch(Dispatchers.Default) {
                for ((key, size) in channelWithObjectSizes) {
                    processed++
                    sizeProcessed += size
                    debug(key, "in channel")
                    if (processed % percentOfTasks == 0) {
                        println("${processed}/$OBJECTS_COUNT, downloaded = $sizeProcessed")
                    }
                }
            }.join();

            sizeProcessed
        }
    }

    val speed = if (getTime.inWholeSeconds == 0L) {
        "${OBJECTS_COUNT.toDouble()/getTime.inWholeMilliseconds.toDouble()}obj/ms"
    } else {
        "${OBJECTS_COUNT/(getTime.inWholeMilliseconds/1000.0)}obj/s"
    }
    println("That's all! $OBJECTS_COUNT objects in ${getTime.inWholeMilliseconds} ($speed), total size: $downloadedSize")
    exitProcess(1)
}

fun debug(taskId: String, msg: String = "") {
    if (DEBUG) {
        println("${Thread.currentThread().name} - req#$taskId $msg")
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

            debug(taskId, "object put to S3")
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
    val getArgs = GetObjectArgs.builder().bucket(BUCKET_NAME).`object`(taskId).build();
    debug(taskId, "Before get object")
    val cf = client.getObject(getArgs);
    val objectResponse = cf.await();
    debug(taskId, "After get response")
    return objectResponse
}