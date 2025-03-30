package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


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

    private val semaphore = Semaphore(parallelRequests)
    private val rateLimiter = RateLimiter(rateLimitPerSec)

    private val maxRetryCount = 2;

    private val averageProcessingTimeCalculator = AverageResponseTimeCalculator(
        initialAverage = requestAverageProcessingTime.toMillis(),
        maxLookupInPastSeconds = 600,
        maxRequestCount = 10_000
    )

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        var retryCount = 0

        while (retryCount <= maxRetryCount) {
            var shouldRetry = false

            semaphore.acquire()

            try {
                if (willCompleteAfterDeadline(deadline)) {
                    logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough parallel requests")
                    paymentESService.update(paymentId) {
                        it.logProcessing(success = false, now(), transactionId, reason = "Request would complete after deadline. No point in processing")
                    }
                    break
                }

                rateLimiter.acquire()

                try {
                    if (willCompleteAfterDeadline(deadline)) {
                        logger.error("[$accountName] Payment would complete after deadline for txId: $transactionId, payment: $paymentId, stage: enough rps tokens")
                        paymentESService.update(paymentId) {
                            it.logProcessing(success = false, now(), transactionId, reason = "Request would complete after deadline. No point in processing")
                        }
                        break
                    }

                    val request = Request.Builder().run {
                        url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                        post(emptyBody)
                    }.build()

                    try {
                        val requestStartTime = now()

                        client.newCall(request).execute().use { response ->
                            val body = try {
                                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            averageProcessingTimeCalculator.onRequestFinished(requestStartTime, now())

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            if (body.result) {
                                return
                            } else {
                                shouldRetry = retryCount < maxRetryCount
                                if (shouldRetry) {
                                    retryCount++
                                }
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
                            else -> {
                                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            }
                        }

                        shouldRetry = retryCount < maxRetryCount
                        if (shouldRetry) {
                            retryCount++
                        }
                    }
                } finally {
                    rateLimiter.release()
                }
            } finally {
                semaphore.release()
            }

            if (!shouldRetry) {
                break
            }
        }
    }

    private fun willCompleteAfterDeadline(deadline: Long): Boolean {
        val expectedEnd = now() + averageProcessingTimeCalculator.getAverage() * 2

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

class AverageResponseTimeCalculator(
    private val initialAverage: Long,
    private val maxLookupInPastSeconds: Long,
    private val maxRequestCount: Long
) {
    private data class Request(val finishedAt: Long, val duration: Long)

    private val requests = ArrayDeque<Request>()

    fun onRequestFinished(start: Long, end: Long) {
        val duration = end - start
        requests.addLast(Request(end, duration))

        while (requests.size > maxRequestCount) {
            requests.removeFirst()
        }
    }

    fun getAverage(): Long {
        cleanup()

        return if (requests.isEmpty()) {
            initialAverage
        } else {
            val total = requests.sumOf { it.duration }
            total / requests.size
        }
    }

    private fun cleanup() {
        val cutoff = now() - maxLookupInPastSeconds * 1_000
        while (requests.isNotEmpty() && requests.first().finishedAt < cutoff) {
            requests.removeFirst()
        }
    }
}