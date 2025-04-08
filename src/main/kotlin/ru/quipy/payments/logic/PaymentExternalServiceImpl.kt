package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
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
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
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

    private val dispatcher = Dispatcher()
    init {
        dispatcher.maxRequests = 1_000
        dispatcher.maxRequestsPerHost = 100
    }
    private val connectionPool = ConnectionPool(
        maxIdleConnections = 10_000,
        keepAliveDuration = 15,
        timeUnit = TimeUnit.MINUTES,
    )
    private val client = OkHttpClient.Builder()
        .dispatcher(dispatcher)
        .connectionPool(connectionPool)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    private val semaphore = Semaphore(parallelRequests)
    private val rateLimiter = RateLimiter(rateLimitPerSec)

    private val maxRetryCount = 4;

    private val processingTimeCalculator = ResponseTimeCalculator(
        initialAverage = requestAverageProcessingTime.toMillis(),
        maxLookupInPastSeconds = 600,
        maxRequestCount = 100_000
    )

    private val executor = Executors.newScheduledThreadPool(1_000)

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        //logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        //logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

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
        semaphore.acquire()

        try {
            if (willCompleteAfterDeadline(deadline)) {
                //logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough parallel requests")
                paymentESService.update(paymentId) {
                    it.logProcessing(success = false, now(), transactionId, reason = "Request would complete after deadline. No point in processing")
                }
                return
            }

            rateLimiter.acquire()

            try {
                if (willCompleteAfterDeadline(deadline)) {
                    //logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough rps tokens")
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

                val delay = processingTimeCalculator.getMedian() * 3
                val cancellationTask = executor.schedule({ call.cancel() }, delay.toLong(), TimeUnit.MILLISECONDS)

                val requestStartTime = now()
                call.execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        //logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    cancellationTask.cancel(true)

                    //logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

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
                        //logger.warn("[$accountName] Payment send request timeout for txId: $transactionId, payment: $paymentId")
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
    private val availableTokens = AtomicInteger(permitsPerSecond)
    private val nextRefillTime = AtomicLong(System.currentTimeMillis() + 1000)
    private val scheduler = Executors.newSingleThreadScheduledExecutor()

    init {
        scheduler.scheduleAtFixedRate(
            ::refillTokens,
            1000,
            1000,
            TimeUnit.MILLISECONDS
        )
    }

    suspend fun acquire() {
        while (true) {
            val current = availableTokens.get()
            if (current > 0 && availableTokens.compareAndSet(current, current - 1)) {
                return
            }

            val now = now()
            val refillAt = nextRefillTime.get()
            val waitTime = refillAt - now

            when {
                waitTime <= 0 -> {
                    delay(1)
                }
                else -> {
                    delay(waitTime.coerceAtLeast(0))
                }
            }
        }
    }

    fun release() {
        availableTokens.updateAndGet { current ->
            (current + 1).coerceAtMost(permitsPerSecond)
        }
    }

    private fun refillTokens() {
        availableTokens.set(permitsPerSecond)
        nextRefillTime.set(now() + 1000)
    }
}

class ResponseTimeCalculator(private val initialAverage: Long, private val maxLookupInPastSeconds: Long, private val maxRequestCount: Long) {
    private data class Request(val finishedAt: Long, val duration: Long)
    private val requests = ConcurrentLinkedDeque<Request>()
    private val requestCount = AtomicInteger(0)
    private val cachedMedian = AtomicLong(initialAverage)
    private val scheduler = Executors.newScheduledThreadPool(2)

    init {
        scheduler.scheduleAtFixedRate(
            ::performCleanup,
            500,
            500,
            TimeUnit.MILLISECONDS
        )
        scheduler.scheduleAtFixedRate(
            ::recalculateMedian,
            1000,
            1000,
            TimeUnit.MILLISECONDS
        )
    }

    fun onRequestFinished(start: Long, end: Long) {
        val duration = end - start
        requests.add(Request(end, duration))
        requestCount.incrementAndGet()
    }

    fun getMedian(): Long {
        return cachedMedian.get()
    }

    private fun performCleanup() {
        val cutoff = now() - maxLookupInPastSeconds * 1_000
        var countRemoved = 0

        while (true) {
            val first = requests.peekFirst() ?: break
            if (first.finishedAt < cutoff) {
                if (requests.pollFirst() != null) countRemoved++ else break
            } else break
        }
        requestCount.addAndGet(-countRemoved)

        var currentCount = requestCount.get()
        while (currentCount > maxRequestCount) {
            if (requests.pollFirst() != null) {
                currentCount = requestCount.decrementAndGet()
            } else break
        }
    }

    private fun recalculateMedian() {
        val snapshot = requests.toList()
        val size = snapshot.size

        cachedMedian.set(
            if (size < 3) initialAverage
            else calculateMedian(snapshot.map { it.duration }, size)
        )
    }

    private fun calculateMedian(sortedDurations: List<Long>, size: Int): Long {
        val sorted = sortedDurations.sorted()
        return if (size % 2 == 1) sorted[size / 2]
        else (sorted[(size / 2) - 1] + sorted[size / 2]) / 2
    }

    private fun now() = System.currentTimeMillis()
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