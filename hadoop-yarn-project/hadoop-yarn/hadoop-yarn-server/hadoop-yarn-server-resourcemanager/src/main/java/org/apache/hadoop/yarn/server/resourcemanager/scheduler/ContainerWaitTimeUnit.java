package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

/**
 * This is the basic data structure used for holding the container wait time for
 * an attempt. The scheduler will get container requests with heart-beats from
 * AMs. The data structure will be updated with every heart beat.
 *
 * All containers are equals while calculating the wait time.
 */
public class ContainerWaitTimeUnit {
  private long latestTimeStamp = 0;
  private int pendingContainers = 0;
  private long totalWaitTime = 0;

  // Do we need capabilities?

  public void addPendingContainers(int containers) {
    this.pendingContainers += containers;
  }

  public void removePendingContainers(int containers) {
    this.pendingContainers -= containers;
  }

  public int getPendingContainers() {
    return this.pendingContainers;
  }

  public void setTimeStampToCurrent() {
    this.latestTimeStamp = System.currentTimeMillis();
  }

  public long getTimeStamp() {
    return this.latestTimeStamp;
  }

  public void setTimeStamp(long timeStamp) {
    this.latestTimeStamp = timeStamp;
  }

  public void addTotalWaitTime(long delta) {
    this.totalWaitTime += delta;
  }

  public long getTotalWaitTime() {
    return this.totalWaitTime;
  }

  public String toString() {
    return "Latest Time Stamp=" + latestTimeStamp + ", Pending Containers="
        + pendingContainers + ", Total wait time= " + totalWaitTime;
  }
}