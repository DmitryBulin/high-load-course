package ru.quipy.payments.logic

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val pool = ThreadPoolExecutor(
        100,
        200,
        15,
        TimeUnit.MINUTES,
        PriorityBlockingQueue(),
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.AbortPolicy()
    )

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        for (account in paymentAccounts) {
            val task = PaymentTask(deadline) {
                account.performPaymentAsync(
                    paymentId,
                    amount,
                    paymentStartedAt,
                    deadline
                )
            }

            pool.execute(task)
        }
    }

    data class PaymentTask(
        val deadline: Long,
        val block: suspend () -> Unit
    ) : Runnable, Comparable<PaymentTask> {
        override fun run() {
            runBlocking {
                try {
                    block()
                } catch (e: Exception) {
                    logger.error("Payment task failed", e)
                }
            }
        }

        override fun compareTo(other: PaymentTask): Int {
            return -(deadline.compareTo(other.deadline))
        }
    }
}

