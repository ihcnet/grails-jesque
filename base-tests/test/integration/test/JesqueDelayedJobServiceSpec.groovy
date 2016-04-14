package test

import net.greghaines.jesque.meta.dao.QueueInfoDAO
import org.joda.time.DateTime
import grails.test.spock.IntegrationSpec

class JesqueDelayedJobServiceSpec extends IntegrationSpec {

    def jesqueDelayedJobService
    def jesqueService
    QueueInfoDAO queueInfoDao
    def failureDao

    void "test enqueue and dequeue"() {
        given:
        def existingProcessedCount = queueInfoDao.processedCount
        def existingFailureCount = failureDao.count
        def queueName = 'testQueue'
        jesqueService.enqueueAt(DateTime.now(), queueName, SimpleJob.simpleName)

        when:
        jesqueDelayedJobService.enqueueReadyJobs()
        jesqueService.withWorker(queueName) {
            sleep(2000)
        }

        then:
        assert existingProcessedCount + 1 == queueInfoDao.processedCount
        assert existingFailureCount == failureDao.count
    }
}
