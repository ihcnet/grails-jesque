package net.greghaines.jesque.worker

import groovy.time.TimeCategory

class TestUtils {
    public static String getRandomUUID() {
        UUID.randomUUID().toString()
    }

    /**
     * Compares two date times
     * @param dateA
     * @param dateB
     * @param threshold - they must be this many milliseconds apart from each other
     * @return
     */
    static boolean fuzzyDateTimeComparison(Date dateA, Date dateB, long threshold = 60 * 1000) {
        use(TimeCategory) {
            def duration = dateA - dateB
            return Math.abs(duration.toMilliseconds()) < threshold
        }
    }

    public static boolean isNullOrEmpty(String value) {
        return value == null ? true : value.isEmpty();
    }
}
