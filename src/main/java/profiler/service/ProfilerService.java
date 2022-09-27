package profiler.service;

import com.sun.management.OperatingSystemMXBean;

import javax.management.MBeanServerConnection;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Date;

public class ProfilerService {
    protected final OperatingSystemMXBean osMBean;

    public ProfilerService() throws IOException {
        MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();
        this.osMBean = ManagementFactory.newPlatformMXBeanProxy(
            mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
    }

    public String getMemory() {
        return "Committed virtual memory size (bytes): " + osMBean.getCommittedVirtualMemorySize()
            + "\nDate: " + new Date(System.currentTimeMillis()).toLocalDate()
            + "\nFree physical memory: " + osMBean.getFreePhysicalMemorySize();
    }
}
