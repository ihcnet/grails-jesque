package grails.plugin.jesque

import net.greghaines.jesque.Job
import net.greghaines.jesque.utils.ReflectionUtils
import net.greghaines.jesque.worker.MapBasedJobFactory
import net.greghaines.jesque.worker.UnpermittedJobException
import org.codehaus.groovy.grails.commons.GrailsApplication

class BeanJobFactory extends MapBasedJobFactory {

    GrailsApplication grailsApplication
    /**
     * Constructor.
     * @param jobTypes the map of job names and types to execute
     */
    BeanJobFactory(Map<String, ? extends Class<?>> jobTypes) {
        super(jobTypes)
    }

    BeanJobFactory(GrailsApplication grailsApplication, Map<String, ? extends Class<?>> jobTypes) {
        this(jobTypes)
        this.grailsApplication = grailsApplication
    }

    @Override
    Object materializeJob(Job job) throws Exception {
        final String className = job.getClassName()
        final Class<?> clazz = jobTypes.get(className)
        if (clazz == null) {
            throw new UnpermittedJobException(className)
        }
        return createInstance(job, className)
    }

    protected Object createInstance(final Job job, String fullClassName) {
        def instance = grailsApplication.mainContext.getBean(fullClassName)
        if (job.vars && !job.vars.isEmpty()) {
            ReflectionUtils.invokeSetters(instance, job.vars)
        }
        return instance
    }

    @Override
    protected void checkJobType(final String jobName, final Class<?> jobType) {
        if (jobName == null) {
            throw new IllegalArgumentException("jobName must not be null")
        }
        if (jobType == null) {
            throw new IllegalArgumentException("jobType must not be null")
        }
    }
}
