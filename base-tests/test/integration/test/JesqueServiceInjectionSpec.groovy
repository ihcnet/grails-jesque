package test

import grails.test.spock.IntegrationSpec
import grails.plugin.redis.RedisService
import grails.plugin.jesque.JesqueService
import net.greghaines.jesque.meta.dao.FailureDAO
import net.greghaines.jesque.meta.dao.QueueInfoDAO
import net.greghaines.jesque.worker.JobFactory
import net.greghaines.jesque.worker.ReflectiveJobFactory

class JesqueServiceInjectionSpec extends IntegrationSpec {

    JesqueService jesqueService
    QueueInfoDAO queueInfoDao
    FailureDAO failureDao
    RedisService redisService

    void setup() {
        redisService.flushDB()
    }

    void tearDown() {
        redisService.flushDB()
    }

    void "test autowirejob with redis service injection with worker"() {
        given:
        def queueName = 'redisAutoWireJob'
        def existingProcessedCount = queueInfoDao.processedCount
        def existingFailureCount = failureDao.count
        redisService.hello = ""
        redisService.worked = ""
        JobFactory jobFactory = new ReflectiveJobFactory()

        when:
        jesqueService.enqueue(queueName, RedisAutoWireJob.simpleName)
        jesqueService.withWorker(queueName, jobFactory) {
            sleep(2000)
        }

        then:
        existingProcessedCount + 1 == queueInfoDao.processedCount
        existingFailureCount == failureDao.count
        redisService.hello == "world"
        redisService.worked == "true"
    }

    void "test autowirejob with redis service injection via config"() {
        given:
        def queueName = 'redisAutoWireJobQueueName'
        def existingProcessedCount = queueInfoDao.processedCount
        def existingFailureCount = failureDao.count
        redisService.hello = ""
        redisService.worked = ""

        when:
        jesqueService.enqueue(queueName, RedisAutoWireJob.simpleName)
        sleep(2000)

        then:
        existingProcessedCount + 1 == queueInfoDao.processedCount
        existingFailureCount == failureDao.count
        redisService.hello == "world"
        redisService.worked == "true"
    }
}
