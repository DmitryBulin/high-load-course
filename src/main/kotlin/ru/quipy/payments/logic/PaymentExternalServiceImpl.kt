package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okio.IOException
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.max
import java.util.PriorityQueue
import kotlin.coroutines.coroutineContext


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val semaphore = PrioritySemaphore(5)
    private val rateLimiter = RateLimiter(rateLimitPerSec)

    private val maxRetryCount = 4;

    private val processingTimeCalculator = ResponseTimeCalculator(
        initialAverage = requestAverageProcessingTime.toMillis(),
        maxLookupInPastSeconds = 600,
        maxRequestCount = 10_000
    )

    private val executor = Executors.newScheduledThreadPool(1_000)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        performPaymentInternalAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, 1)
    }

    private suspend fun performPaymentInternalAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, transactionId: UUID, tryNumber: Int) {
        if (tryNumber > maxRetryCount) {
            return
        }

        var shouldRetry = false
        var shouldWaitTillDeadline = false
        semaphore.acquire(deadline)

        try {
            if (willCompleteAfterDeadline(deadline)) {
                logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough parallel requests")
                paymentESService.update(paymentId) {
                    it.logProcessing(success = false, now(), transactionId, reason = "Request would complete after deadline. No point in processing")
                }
                return
            }

            rateLimiter.acquire()

            try {
                if (willCompleteAfterDeadline(deadline)) {
                    logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough rps tokens")
                    paymentESService.update(paymentId) {
                        it.logProcessing(success = false, now(), transactionId, reason = "Request would complete after deadline. No point in processing")
                    }
                    rateLimiter.release()
                    return
                }

                val request = Request.Builder().run {
                    url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                val call = client.newCall(request)

                val delay = processingTimeCalculator.getMedian() * 2
                val cancellationTask = executor.schedule({ call.cancel() }, delay.toLong(), TimeUnit.MILLISECONDS)

                val requestStartTime = now()
                call.execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    cancellationTask.cancel(true)

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        processingTimeCalculator.onRequestFinished(requestStartTime, now())
                        return
                    } else {
                        shouldRetry = true
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }
                    is IOException -> {
                        logger.warn("[$accountName] Payment send request timeout for txId: $transactionId, payment: $paymentId")
                        shouldRetry = true
                        shouldWaitTillDeadline = true
                    }
                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        } finally {
            if (shouldRetry) {
                if (shouldWaitTillDeadline) {
                    performPaymentInternalAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, tryNumber + 1)
                    val delay = max(0, deadline - now())
                    Thread.sleep(delay)
                    semaphore.release()
                } else {
                    semaphore.release()
                    with(CoroutineScope(coroutineContext)) {
                        launch {
                            performPaymentInternalAsync(paymentId, amount, paymentStartedAt, deadline, transactionId, tryNumber + 1)
                        }
                    }
                }
            } else {
                semaphore.release()
            }
        }
    }

    private fun willCompleteAfterDeadline(deadline: Long): Boolean {
        val expectedEnd = now() + processingTimeCalculator.getMedian() * 2

        return expectedEnd >= deadline
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

class RateLimiter(private val permitsPerSecond: Int) {
    private var availableTokens = permitsPerSecond
    private var nextRefillTime = now() + 1000
    private val mutex = Mutex()

    suspend fun acquire() {
        mutex.withLock {
            while (true) {
                refillTokens()
                when {
                    availableTokens > 0 -> {
                        availableTokens--
                        return
                    }
                    else -> {
                        val waitTime = nextRefillTime - now()
                        if (waitTime > 0) delay(waitTime)
                    }
                }
            }
        }
    }

    suspend fun release() {
        mutex.withLock {
            availableTokens++
            if (availableTokens > permitsPerSecond) {
                availableTokens--
            }
        }
    }

    private fun refillTokens() {
        val now = now()
        if (now >= nextRefillTime) {
            availableTokens = permitsPerSecond
            nextRefillTime = now + 1000
        }
    }
}

class ResponseTimeCalculator(private val initialAverage: Long, private val maxLookupInPastSeconds: Long, private val maxRequestCount: Long) {
    private data class Request(val finishedAt: Long, val duration: Long)

    private val requests = ArrayDeque<Request>()

    fun onRequestFinished(start: Long, end: Long) {
        val duration = end - start
        requests.addLast(Request(end, duration))

        while (requests.size > maxRequestCount) {
            requests.removeFirst()
        }
    }

    fun getMedian(): Long {
        cleanup()

        if (requests.count() < 3) return initialAverage

        val sorted = requests.map { it.duration }.sorted()

        val size = sorted.size

        return if (size % 2 == 1) {
            sorted[size / 2]
        } else {
            (sorted[(size / 2) - 1] + sorted[size / 2]) / 2
        }
    }

    private fun cleanup() {
        val cutoff = now() - maxLookupInPastSeconds * 1_000
        while (requests.isNotEmpty() && requests.first().finishedAt < cutoff) {
            requests.removeFirst()
        }
    }
}

class PrioritySemaphore(permits: Int) {
    private val mutex = Mutex()
    private var availablePermits = permits
    private val waitingQueue = PriorityQueue<AwaitingRequest>(compareBy { it.deadline })

    private class AwaitingRequest(val deadline: Long, val deferred: CompletableDeferred<Unit>)

    suspend fun acquire(deadline: Long) {
        val deferred: CompletableDeferred<Unit>?
        mutex.lock()
        try {
            if (availablePermits > 0) {
                availablePermits--
                return
            } else {
                deferred = CompletableDeferred()
                waitingQueue.add(AwaitingRequest(deadline, deferred))
            }
        } finally {
            mutex.unlock()
        }
        deferred!!.await()
    }

    suspend fun release() {
        mutex.lock()
        try {
            availablePermits++
            while (availablePermits > 0 && waitingQueue.isNotEmpty()) {
                val nextRequest = waitingQueue.poll()
                availablePermits--
                nextRequest.deferred.complete(Unit)
            }
        } finally {
            mutex.unlock()
        }
    }
}