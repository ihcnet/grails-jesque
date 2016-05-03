package net.greghaines.jesque.worker

import groovy.json.JsonSlurper
import net.greghaines.jesque.Config
import net.greghaines.jesque.Job
import net.greghaines.jesque.utils.JesqueUtils
import net.greghaines.jesque.utils.VersionUtils
import org.apache.commons.collections4.CollectionUtils
import org.spockframework.gentyref.TypeToken
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisException
import redis.clients.util.Pool
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.lang.reflect.UndeclaredThrowableException
import java.text.SimpleDateFormat
import java.util.concurrent.Callable
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import static net.greghaines.jesque.utils.ResqueConstants.*
import static net.greghaines.jesque.worker.JobExecutor.State.*
import static net.greghaines.jesque.worker.WorkerEvent.*

class WorkerJedisPoolImplSpec extends Specification {

    @Shared
    ArrayList<String> queues

    Config config

    Pool<Jedis> jedisPool

    Jedis jedis

    JobFactory jobFactory

    @Shared
    String namespace

    @Shared
    SimpleDateFormat jesqueSimpleDateFormat

    def setupSpec() {
        queues = ['foo', 'bar']
        namespace = "fuori"
        jesqueSimpleDateFormat = new SimpleDateFormat(DATE_FORMAT)
    }

    def setup() {
        config = Mock(Config)
        jedisPool = Mock(type: new TypeToken<Pool<Jedis>>() {}.type) as Pool<Jedis>
        jedis = Mock(Jedis)
        jobFactory = Mock(JobFactory)

    }

    @Unroll
    def "test isThreadNameChangingEnabled with expectedEnabled = #expectedEnabled"() {
        given:
        WorkerJedisPoolImpl.setThreadNameChangingEnabled(expectedEnabled)

        expect:
        expectedEnabled == WorkerJedisPoolImpl.isThreadNameChangingEnabled()

        where:
        expectedEnabled << [true, false]
    }

    @Unroll
    def "test checkQueues bad cases with queues = #queues"() {
        when:
        WorkerJedisPoolImpl.checkQueues(queues)

        then:
        thrown(IllegalArgumentException)

        where:
        queues << [null, [], [""], [null], ["", null]]
    }

    @Unroll
    def "test end(#endNow)"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> namespace

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        final ExecutorService executorService = Executors.newWorkStealingPool()

        and:
        def expectedState = endNow ? SHUTDOWN_IMMEDIATE : SHUTDOWN

        when:
        executorService.submit(workerJedisPool)
        workerJedisPool.end(endNow)

        then:
        workerJedisPool.state.get() == expectedState
        workerJedisPool.isShutdown()
        !workerJedisPool.paused

        where:
        endNow << [true, false]
    }

    def "test isProcessingJob"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> namespace
        Job job = Mock(Job)

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        workerJedisPool.workerEventEmitter.addListener(new WorkerListener() {
            @Override
            void onEvent(WorkerEvent event, Worker worker, String queue, Job j, Object runner, Object result, Throwable t) {
                assert worker.processingJob
            }
        })

        when:
        workerJedisPool.process(job, queues.first())

        then:
        1 == 1
    }

    @Unroll
    def "test togglePause(#expectedPaused)"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> namespace

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        when:
        workerJedisPool.togglePause(expectedPaused)

        then:
        expectedPaused == workerJedisPool.isPaused()
        expectedPaused == workerJedisPool.paused

        where:
        expectedPaused << [true, false]
    }

    def "test getName"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        expect:
        workerJedisPool.name
        workerJedisPool.name == workerJedisPool.createName()
    }

    def "test getWorkerEventEmitter"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        expect:
        null != workerJedisPool.workerEventEmitter
    }

    def "test getQueues"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        expect:
        CollectionUtils.disjunction(queues, workerJedisPool.queues).empty
    }

    def "test addQueue"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        and:
        String newQueueName = TestUtils.randomUUID
        List<String> expectedQueues = queues.collect()
        expectedQueues.add(newQueueName)

        when:
        workerJedisPool.addQueue(newQueueName)

        then:
        CollectionUtils.disjunction(expectedQueues, workerJedisPool.queues).empty
    }

    @Unroll
    def "test addQueue bad queue name"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        when:
        workerJedisPool.addQueue(badQueueName)

        then:
        thrown(IllegalArgumentException)

        where:
        badQueueName << ["", null]
    }

    @Unroll
    def "test removeQueue with all = #removeAll"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        def queues = [
                "repeated",
                "repeated",
                "repeated",
                "somethingElse"
        ]

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        and:
        String queueToRemove = queues.first()
        List<String> expectedQueues = queues.collect()
        if (removeAll) {
            expectedQueues.removeAll {
                it == queueToRemove
            }
        } else {
            expectedQueues.remove(queueToRemove)
        }

        when:
        workerJedisPool.removeQueue(queueToRemove, removeAll)

        then:
        CollectionUtils.disjunction(expectedQueues, workerJedisPool.queues).empty

        where:
        removeAll << [true, false]

    }

    @Unroll
    def "test bad queue name removeQueue(#badQueueName, #removeAll)"(boolean removeAll, String badQueueName) {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        def queues = [
                "repeated",
                "repeated",
                "repeated",
                "somethingElse"
        ]

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        when:
        workerJedisPool.removeQueue(badQueueName, removeAll)

        then:
        thrown(IllegalArgumentException)

        where:
        [removeAll, badQueueName] << [[true, false], ["", null]].combinations()

    }

    def "test removeAllQueues"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        and:
        String newQueueName = TestUtils.randomUUID
        List<String> expectedQueues = queues.collect()
        expectedQueues.add(newQueueName)

        when:
        workerJedisPool.removeAllQueues()

        then:
        workerJedisPool.queues.empty
    }

    def "test setQueues"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        and:
        Collection<String> queueNames = [
                TestUtils.randomUUID,
                TestUtils.randomUUID,
                TestUtils.randomUUID
        ]

        when:
        workerJedisPool.setQueues(queueNames)

        then:
        CollectionUtils.disjunction(queueNames, workerJedisPool.queues).empty
    }

    def "test setQueues with bad queue names"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        and:
        List<String> queueNames = [badQueueName]

        when:
        workerJedisPool.setQueues(queueNames)

        then:
        thrown(IllegalArgumentException)

        where:
        badQueueName << ["", null]
    }

    def "test setQueues exception on jedis.close"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Exception expectedException = new Exception("Test exception")

        and:
        Collection<String> queueNames = [
                TestUtils.randomUUID,
                TestUtils.randomUUID,
                TestUtils.randomUUID
        ]

        when:
        workerJedisPool.setQueues(queueNames)

        then:
        RuntimeException actualException = thrown(RuntimeException)
        expectedException == actualException.cause
        jedis.close() >> {
            throw expectedException
        }
    }

    def "test getJobFactory"() {
        expect:
        jobFactory == buildDefaultWJPI().jobFactory
    }

    def "test getExceptionHandler"() {
        expect:
        buildDefaultWJPI().exceptionHandler instanceof DefaultExceptionHandler
    }

    def "test setExceptionHandler"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        ExceptionHandler testExceptionHandler = Mock(ExceptionHandler)

        when:
        workerJedisPool.setExceptionHandler(testExceptionHandler)

        then:
        workerJedisPool.exceptionHandler == testExceptionHandler
    }

    def "test setExceptionHandler with null"() {
        when:
        buildDefaultWJPI().setExceptionHandler(null)

        then:
        IllegalArgumentException illegalArgumentException = thrown(IllegalArgumentException)
        illegalArgumentException.message == 'exceptionHandler must not be null'
    }

    def "test join"() {
        given: 'mock interactions'
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> namespace

        and: 'create a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )

        final ExecutorService executorService = Executors.newWorkStealingPool()

        when:
        executorService.submit(workerJedisPool)
        workerJedisPool.join(10l)

        then:
        notThrown(Exception)
    }

    def "test getReconnectAttempts"() {
        expect:
        WorkerJedisPoolImpl.RECONNECT_ATTEMPTS == buildDefaultWJPI().reconnectAttempts
    }

    def "test pop"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String queueName = queues.first()
        String expectedKey = workerJedisPool.key(QUEUE, queueName)
        String expectedPopResult = TestUtils.randomUUID
        String expectedRecurringHashKey = JesqueUtils.createRecurringHashKey(expectedKey)
        String inflightKey = workerJedisPool.key(INFLIGHT, workerJedisPool.name, queueName)
        Long currentMilliseconds = System.currentTimeMillis()
        1 * jedis.evalsha(
                _,
                _,
                _,
                _,
                _,
                _
        ) >> {
            a ->
                assert a.size() == 3
                def sha1 = a[0] as String
                def keyCount = a[1] as Integer
                def params = a[2] as String[]
                assert null == sha1
                assert 3 == keyCount
                assert params
                assert params.size() == 4
                assert expectedKey == params[0]
                assert inflightKey == params[1]
                assert expectedRecurringHashKey == params[2]
                assert params[3]
                Long actualMilliseconds = Long.parseLong(params[3])
                assert Math.abs(actualMilliseconds - currentMilliseconds) < 1000
                return expectedPopResult
        }

        expect:
        expectedPopResult == workerJedisPool.pop(queueName)
    }

    def "test pop with jedis close failure"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String queueName = queues.first()
        String expectedKey = workerJedisPool.key(QUEUE, queueName)
        String expectedPopResult = TestUtils.randomUUID
        String expectedRecurringHashKey = JesqueUtils.createRecurringHashKey(expectedKey)
        String inflightKey = workerJedisPool.key(INFLIGHT, workerJedisPool.name, queueName)
        Long currentMilliseconds = System.currentTimeMillis()
        Exception expectedException = new Exception("Test exception")


        when:
        workerJedisPool.pop(queueName)

        then:
        RuntimeException actualExcception = thrown(RuntimeException)
        expectedException == actualExcception.cause
        1 * jedis.evalsha(
                _,
                _,
                _,
                _,
                _,
                _
        ) >> {
            a ->
                assert a.size() == 3
                def sha1 = a[0] as String
                def keyCount = a[1] as Integer
                def params = a[2] as String[]
                assert null == sha1
                assert 3 == keyCount
                assert params
                assert params.size() == 4
                assert expectedKey == params[0]
                assert inflightKey == params[1]
                assert expectedRecurringHashKey == params[2]
                assert params[3]
                Long actualMilliseconds = Long.parseLong(params[3])
                assert Math.abs(actualMilliseconds - currentMilliseconds) < 1000
                return expectedPopResult
        }
        1 * jedis.close() >> {
            throw expectedException
        }
    }


    @Unroll
    def "test recoverFromException with recoveryStrategy: #recoveryStrategy"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        def exceptionHandler = Mock(ExceptionHandler)
        Exception exception = new Exception()
        String currentQueue = queues.first()
        1 * exceptionHandler.onException(_ as JobExecutor, _ as Exception, _ as String) >> {
            JobExecutor jobExecutor, Exception pException, String curQueue ->
                assert exception == pException
                assert workerJedisPool == jobExecutor
                assert currentQueue == curQueue
                return recoveryStrategy
        }

        workerJedisPool.setExceptionHandler(exceptionHandler)
        def expectedState = recoveryStrategy == RecoveryStrategy.PROCEED ? NEW : SHUTDOWN
        def expectedShutdown = recoveryStrategy != RecoveryStrategy.PROCEED

        when:
        workerJedisPool.recoverFromException(currentQueue, exception)

        then:
        expectedState == workerJedisPool.state.get()
        expectedShutdown == workerJedisPool.isShutdown()
        !workerJedisPool.paused

        where:
        recoveryStrategy << RecoveryStrategy.values()
    }

    def "test checkPaused not paused"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        workerJedisPool.togglePause(false)

        when:
        workerJedisPool.checkPaused()

        then:
        1 * jedis.close()
    }

    def "test checkPaused paused"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        final ExecutorService executorService = Executors.newWorkStealingPool()
        executorService.submit(workerJedisPool)
        workerJedisPool.togglePause(true)

        when:
        workerJedisPool.checkPaused()

        then:
        1 * jedis.set(workerJedisPool.key(WORKER, workerJedisPool.name), _ as String) >> {
            String key, String value ->
                workerJedisPool.togglePause(false)
        }
        _ * jedis.del(workerJedisPool.key(WORKER, workerJedisPool.name))

        cleanup:
        workerJedisPool.end(true)
    }

    def "test checkPaused throw exception on jedis close"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        workerJedisPool.togglePause(false)
        Exception expectedException = new Exception("Test exception")

        when:
        workerJedisPool.checkPaused()

        then:
        RuntimeException actualException = thrown(RuntimeException)
        expectedException == actualException.cause
        1 * jedis.close() >> {
            throw expectedException
        }
    }

    def "test process"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable runnable = Mock(Runnable)

        when:
        workerJedisPool.process(job, currentQueue)

        then:
        1 * workerListener.onEvent(JOB_PROCESS, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_SUCCESS, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * jobFactory.materializeJob(job) >> {
            return runnable
        }
        1 * jedis.del(workerJedisPool.key(WORKER, workerJedisPool.name)) >> {
            assert workerJedisPool.processingJob
        }
        1 * runnable.run()
    }

    def "test process Runnable throws exception"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable runnable = Mock(Runnable)
        Exception e = new Exception("Test exception message")

        when:
        workerJedisPool.process(job, currentQueue)

        then:
        1 * workerListener.onEvent(JOB_PROCESS, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_FAILURE, _ as Worker, _ as String, _ as Job, _, _, _ as Throwable) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
                assert e == ((UndeclaredThrowableException) t).undeclaredThrowable
        }
        1 * jobFactory.materializeJob(job) >> {
            return runnable
        }
        1 * jedis.del(workerJedisPool.key(WORKER, workerJedisPool.name)) >> {
            assert workerJedisPool.processingJob
        }
        1 * runnable.run() >> {
            throw e
        }
    }

    def "test process throws exception"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable runnable = Mock(Runnable)
        Exception e = new Exception("A test exception message.")

        when:
        workerJedisPool.process(job, currentQueue)

        then:
        thrown(RuntimeException)
        1 * workerListener.onEvent(JOB_PROCESS, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerListener.onEvent(JOB_SUCCESS, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * jobFactory.materializeJob(job) >> {
            return runnable
        }
        1 * jedis.del(workerJedisPool.key(WORKER, workerJedisPool.name)) >> {
            assert workerJedisPool.processingJob
        }
        jedis.close() >> {} >> {} >> {
            throw e
        }
    }

    def "test execute with runnable job"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable runnable = Mock(Runnable)

        when:
        workerJedisPool.execute(job, currentQueue, runnable)

        then:
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * runnable.run()
        0 * _
    }

    def "test execute with callable job"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Callable<String> callable = Mock(type: new TypeToken<Callable<String>>() {}.type) as Callable<String>
        String expectedResult = TestUtils.randomUUID

        when:
        String actualResult = workerJedisPool.execute(job, currentQueue, callable)

        then:
        actualResult
        expectedResult == actualResult
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * callable.call() >> expectedResult
        0 * _
    }

    def "test execute WorkerAware"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        TestWorkerAwareJob workerAware = Mock(TestWorkerAwareJob)

        when:
        workerJedisPool.execute(job, currentQueue, workerAware)

        then:
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        1 * workerAware.run()
        1 * workerAware.setWorker(_ as Worker) >> {
            Worker worker ->
                assert workerJedisPool == worker
        }
        0 * _
    }

    def "test execute unknown interface"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Object instance = Mock()

        when:
        workerJedisPool.execute(job, currentQueue, instance)

        then:
        1 * workerListener.onEvent(JOB_EXECUTE, _ as Worker, _ as String, _ as Job, _, _, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
        }
        ClassCastException classCastException = thrown(ClassCastException)
        classCastException.message.startsWith("Instance must be a Runnable or a Callable: ")
        0 * _
    }

    def "test success"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable instance = Mock(Runnable)
        String expectedResult = TestUtils.randomUUID

        when:
        workerJedisPool.success(job, instance, expectedResult, currentQueue)

        then:
        1 * jedis.incr(workerJedisPool.key(STAT, PROCESSED))
        1 * jedis.incr(workerJedisPool.key(STAT, PROCESSED, workerJedisPool.name))
        1 * workerListener.onEvent(JOB_SUCCESS, _ as Worker, _ as String, _ as Job, _ as Object, _ as Object, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
                assert instance == runner
                assert expectedResult == result
        }
    }

    def "test success jedis exception on incr"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable instance = Mock(Runnable)
        String expectedResult = TestUtils.randomUUID
        JedisException jedisException = new JedisException("Oh no incr for you.")

        when:
        workerJedisPool.success(job, instance, expectedResult, currentQueue)

        then:
        1 * jedis.incr(workerJedisPool.key(STAT, PROCESSED)) >> {
            throw jedisException
        }
    }

    def "test success error on close jedis"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Runnable instance = Mock(Runnable)
        String expectedResult = TestUtils.randomUUID
        Exception e = new Exception("A scary message.")

        when:
        workerJedisPool.success(job, instance, expectedResult, currentQueue)

        then:
        RuntimeException actualException = thrown(RuntimeException)
        e == actualException.cause
        1 * jedis.incr(workerJedisPool.key(STAT, PROCESSED))
        1 * jedis.incr(workerJedisPool.key(STAT, PROCESSED, workerJedisPool.name))
        1 * workerListener.onEvent(JOB_SUCCESS, _ as Worker, _ as String, _ as Job, _ as Object, _ as Object, _) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
                assert instance == runner
                assert expectedResult == result
        }
        jedis.close() >> {
            throw e
        }
    }

    def "test failure"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Throwable expectedThrowable = new Throwable("the original cause")

        when:
        workerJedisPool.failure(expectedThrowable, job, currentQueue)

        then:
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED))
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED, workerJedisPool.name))
        1 * jedis.rpush(workerJedisPool.key(FAILED), _ as String)
        1 * workerListener.onEvent(JOB_FAILURE, _ as Worker, _ as String, _ as Job, _, _, _ as Throwable) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
                assert null == runner
                assert null == result
                assert expectedThrowable == t
        }
    }

    def "test failure JedisException on incr"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Throwable expectedThrowable = new Throwable("the original cause")
        JedisException jedisException = new JedisException("some jedis exception")

        when:
        workerJedisPool.failure(expectedThrowable, job, currentQueue)

        then:
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED)) >> {
            throw jedisException
        }
    }

    def "test failure IOException on incr"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Throwable expectedThrowable = new Throwable("the original cause")
        IOException ioException = new IOException("some i/o exception")

        when:
        workerJedisPool.failure(expectedThrowable, job, currentQueue)

        then:
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED)) >> {
            throw ioException
        }
    }

    def "test failure exception on close of jedis"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Job job = Mock(Job)
        String currentQueue = queues.first()
        WorkerListener workerListener = Mock(WorkerListener)
        workerJedisPool.workerEventEmitter.addListener(workerListener)
        Throwable expectedThrowable = new Throwable("the original cause")
        Exception expectedException = new Exception("Fake exception during jedis close")

        when:
        workerJedisPool.failure(expectedThrowable, job, currentQueue)

        then:
        RuntimeException actualException = thrown(RuntimeException)
        expectedException == actualException.cause
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED))
        1 * jedis.incr(workerJedisPool.key(STAT, FAILED, workerJedisPool.name))
        1 * jedis.rpush(workerJedisPool.key(FAILED), _ as String)
        1 * workerListener.onEvent(JOB_FAILURE, _ as Worker, _ as String, _ as Job, _, _, _ as Throwable) >> {
            WorkerEvent event, Worker worker, String queue, Job pJob,
            Object runner, Object result, Throwable t ->
                assert workerJedisPool == worker
                assert currentQueue == queue
                assert job == pJob
                assert null == runner
                assert null == result
                assert expectedThrowable == t
        }
        jedis.close() >> {
            throw expectedException
        }
    }

    def "test failMsg"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        Exception exception = new Exception()
        String queue = queues.first()
        Job job = Mock(Job)
        def fakeArgs = [
                TestUtils.randomUUID,
                TestUtils.randomUUID
        ]
        def fakeVars = [
                (TestUtils.randomUUID): TestUtils.randomUUID,
                (TestUtils.randomUUID): TestUtils.randomUUID
        ]
        job.args >> (fakeArgs as Object[])
        job.vars >> (fakeVars as Map<String, Object>)

        when:
        String message = workerJedisPool.failMsg(exception, queue, job)
        JsonSlurper jsonSlurper = new JsonSlurper()
        def parsedMessage = jsonSlurper.parse(new StringReader(message))

        then:
        message
        parsedMessage
        workerJedisPool.name == parsedMessage.worker
        exception.class.name == parsedMessage.exception
        queue == parsedMessage.queue
        parsedMessage.backtrace
        parsedMessage.payload
        fakeArgs == parsedMessage.payload.args
        fakeVars == parsedMessage.payload.vars
        parsedMessage.failed_at
        TestUtils.fuzzyDateTimeComparison(jesqueSimpleDateFormat.parse(parsedMessage.failed_at), new Date())
    }

    def "test statusMsg"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String queue = queues.first()
        Job job = Mock(Job)
        def fakeArgs = [
                TestUtils.randomUUID,
                TestUtils.randomUUID
        ]
        def fakeVars = [
                (TestUtils.randomUUID): TestUtils.randomUUID,
                (TestUtils.randomUUID): TestUtils.randomUUID
        ]
        job.args >> (fakeArgs as Object[])
        job.vars >> (fakeVars as Map<String, Object>)

        when:
        String message = workerJedisPool.statusMsg(queue, job)
        JsonSlurper jsonSlurper = new JsonSlurper()
        def parsedMessage = jsonSlurper.parse(new StringReader(message))

        then:
        message
        parsedMessage
        TestUtils.isNullOrEmpty(parsedMessage.worker)
        TestUtils.isNullOrEmpty(parsedMessage.exception)
        queue == parsedMessage.queue
        TestUtils.isNullOrEmpty(parsedMessage.backtrace)
        parsedMessage.payload
        job.args == parsedMessage.payload.args
        job.vars == parsedMessage.payload.vars
        TestUtils.isNullOrEmpty(parsedMessage.failed_at)
        TestUtils.fuzzyDateTimeComparison(jesqueSimpleDateFormat.parse(parsedMessage.run_at), new Date())
    }

    def "test pauseMsg"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        when:
        String message = workerJedisPool.pauseMsg()
        JsonSlurper jsonSlurper = new JsonSlurper()
        def parsedMessage = jsonSlurper.parse(new StringReader(message))

        then:
        message
        parsedMessage
        TestUtils.isNullOrEmpty(parsedMessage.exception)
        TestUtils.isNullOrEmpty(parsedMessage.queue)
        TestUtils.isNullOrEmpty(parsedMessage.backtrace)
        TestUtils.isNullOrEmpty(parsedMessage.failed_at)
        TestUtils.fuzzyDateTimeComparison(jesqueSimpleDateFormat.parse(parsedMessage.run_at), new Date())
        workerJedisPool.paused == parsedMessage.paused
    }

    def "test createName"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        when:
        String name = workerJedisPool.createName()
        then:
        workerJedisPool.name == name
    }


    def "test key"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        def parts = [
                TestUtils.randomUUID,
                TestUtils.randomUUID,
                TestUtils.randomUUID
        ]

        expect:
        JesqueUtils.createKey(workerJedisPool.namespace, parts) == workerJedisPool.key(parts as String[])
    }

    def "test renameThread"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String newThreadName = TestUtils.randomUUID
        String expectedThreadName = "Worker-$workerJedisPool.workerId Jesque-${VersionUtils.getVersion()}: $newThreadName"

        when:
        workerJedisPool.renameThread(newThreadName)

        then:
        expectedThreadName == Thread.currentThread().name
    }

    def "test lpoplpush"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String expectedResult = TestUtils.randomUUID
        String from = TestUtils.randomUUID
        String to = TestUtils.randomUUID
        1 * jedis.evalsha(
                _,
                _,
                _,
                _,
        ) >> {
            a ->
                assert a.size() == 3
                def sha1 = a[0] as String
                def keyCount = a[1] as Integer
                def params = a[2] as String[]
                assert null == sha1
                assert 2 == keyCount
                assert params
                assert params.size() == 2
                assert from == params[0]
                assert to == params[1]
                return expectedResult
        }

        expect:
        expectedResult == workerJedisPool.lpoplpush(from, to)
    }

    def "test lpoplpush jedis exception on close"() {
        given: 'a workerJedisPool instance'
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()
        String expectedResult = TestUtils.randomUUID
        String from = TestUtils.randomUUID
        String to = TestUtils.randomUUID
        Exception expectedException = new Exception("A test exception message")

        when:
        workerJedisPool.lpoplpush(from, to)

        then:
        RuntimeException actualException = thrown(RuntimeException)
        expectedException == actualException.cause
        1 * jedis.evalsha(
                _,
                _,
                _,
                _,
        ) >> {
            a ->
                assert a.size() == 3
                def sha1 = a[0] as String
                def keyCount = a[1] as Integer
                def params = a[2] as String[]
                assert null == sha1
                assert 2 == keyCount
                assert params
                assert params.size() == 2
                assert from == params[0]
                assert to == params[1]
                return expectedResult
        }
        1 * jedis.close() >> {
            throw expectedException
        }
    }

    def "test toString"() {
        given:
        WorkerJedisPoolImpl workerJedisPool = buildDefaultWJPI()

        expect:
        "${workerJedisPool.namespace}${COLON}${WORKER}${COLON}${workerJedisPool.name}".toString() == workerJedisPool.toString()
    }

    private WorkerJedisPoolImpl buildDefaultWJPI() {
        jedisPool.getResource() >> jedis
        1 * config.getNamespace() >> "fuori"

        WorkerJedisPoolImpl workerJedisPool = new WorkerJedisPoolImpl(
                config,
                queues,
                jobFactory,
                jedisPool
        )
        return workerJedisPool
    }

}