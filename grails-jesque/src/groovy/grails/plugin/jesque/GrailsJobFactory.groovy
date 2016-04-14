package grails.plugin.jesque

import net.greghaines.jesque.Job
import net.greghaines.jesque.utils.ReflectionUtils
import net.greghaines.jesque.worker.JobFactory
import net.greghaines.jesque.worker.UnpermittedJobException
import org.codehaus.groovy.grails.commons.GrailsApplication

/**
 * Job Factory that knows how to materialize grails jobs.
 */
class GrailsJobFactory implements JobFactory {

    GrailsApplication grailsApplication

    public GrailsJobFactory(final GrailsApplication grailsApplication) {
        this.grailsApplication = grailsApplication
    }

    @Override
    public Object materializeJob(final Job job) throws Exception {
        Class jobClass = QueueConfiguration.jobTypes.get(job.className)
        if (jobClass == null) {
            throw new UnpermittedJobException(job.className)
        }
        return createInstance(job, jobClass.canonicalName)
    }

    protected Object createInstance(final Job job, final String fullClassName) {
        def instance = grailsApplication.mainContext.getBean(fullClassName)
        if (job.vars && !job.vars.isEmpty()) {
            ReflectionUtils.invokeSetters(instance, job.vars)
        }
        return instance
    }
}
