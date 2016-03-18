package pt.tecnico.spark.util;

/**
 * POJO to save task runtime
 */
public class TaskRuntimeStatistic {
    private Integer stageId;
    private Long average;
    private Long fastest;
    private Long slowest;
    private Long standardDeviation;
    private String name;
    private Integer taskCount;
    private Long percent5;
    private Long percent25;
    private Long median;
    private Long percent75;
    private Long percent95;
    private Long totalTaskRuntime;
    private Long stageRuntime;
    private Long fetchWaitTime;
    private Long shuffleWriteTime;
    private Long startTime;
    private Long completionTime;

    public TaskRuntimeStatistic() {
    }

    public TaskRuntimeStatistic(Integer stageId,
                                Long average,
                                Long fastest,
                                Long slowest,
                                Long standardDeviation,
                                String name,
                                Integer taskCount,
                                Long percent5,
                                Long percent25,
                                Long median,
                                Long percent75,
                                Long percent95,
                                Long totalTaskRuntime,
                                Long stageRuntime,
                                Long fetchWaitTime,
                                Long shuffleWriteTime) {
        this.stageId = stageId;
        this.average = average;
        this.fastest = fastest;
        this.slowest = slowest;
        this.standardDeviation = standardDeviation;
        this.name = name;
        this.taskCount = taskCount;
        this.percent5 = percent5;
        this.percent25 = percent25;
        this.median = median;
        this.percent75 = percent75;
        this.percent95 = percent95;
        this.totalTaskRuntime = totalTaskRuntime;
        this.stageRuntime = stageRuntime;
        this.fetchWaitTime = fetchWaitTime;
        this.shuffleWriteTime = shuffleWriteTime;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(Long completionTime) {
        this.completionTime = completionTime;
    }

    public Long getFetchWaitTime() {
        return fetchWaitTime;
    }

    public void setFetchWaitTime(Long fetchWaitTime) {
        this.fetchWaitTime = fetchWaitTime;
    }

    public Long getShuffleWriteTime() {
        return shuffleWriteTime;
    }

    public void setShuffleWriteTime(Long shuffleWriteTime) {
        this.shuffleWriteTime = shuffleWriteTime;
    }

    public Long getTotalTaskRuntime() {
        return totalTaskRuntime;
    }

    public void setTotalTaskRuntime(Long totalTaskRuntime) {
        this.totalTaskRuntime = totalTaskRuntime;
    }

    public Long getStageRuntime() {
        return stageRuntime;
    }

    public void setStageRuntime(Long stageRuntime) {
        this.stageRuntime = stageRuntime;
    }

    public Long getPercent5() {
        return percent5;
    }

    public void setPercent5(Long percent5) {
        this.percent5 = percent5;
    }

    public Long getPercent95() {
        return percent95;
    }

    public void setPercent95(Long percent95) {
        this.percent95 = percent95;
    }

    public Long getPercent25() {
        return percent25;
    }

    public void setPercent25(Long percent25) {
        this.percent25 = percent25;
    }

    public Long getMedian() {
        return median;
    }

    public void setMedian(Long median) {
        this.median = median;
    }

    public Long getPercent75() {
        return percent75;
    }

    public void setPercent75(Long percent75) {
        this.percent75 = percent75;
    }

    public Integer getTaskCount() {
        return taskCount;
    }

    public void setTaskCount(Integer taskCount) {
        this.taskCount = taskCount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public Long getAverage() {
        return average;
    }

    public void setAverage(Long average) {
        this.average = average;
    }

    public Long getFastest() {
        return fastest;
    }

    public void setFastest(Long fastest) {
        this.fastest = fastest;
    }

    public Long getSlowest() {
        return slowest;
    }

    public void setSlowest(Long slowest) {
        this.slowest = slowest;
    }

    public Long getStandardDeviation() {
        return standardDeviation;
    }

    public void setStandardDeviation(Long standardDeviation) {
        this.standardDeviation = standardDeviation;
    }

    @Override
    public String toString() {
        return "TaskRuntimeStatistic{" +
                "stageId=" + stageId +
                ", average=" + average +
                ", fastest=" + fastest +
                ", slowest=" + slowest +
                ", standardDeviation=" + standardDeviation +
                ", name='" + name + '\'' +
                ", taskCount=" + taskCount +
                ", percent5=" + percent5 +
                ", percent25=" + percent25 +
                ", median=" + median +
                ", percent75=" + percent75 +
                ", percent95=" + percent95 +
                ", totalTaskRuntime=" + totalTaskRuntime +
                ", stageRuntime=" + stageRuntime +
                ", fetchWaitTime=" + fetchWaitTime +
                ", shuffleWriteTime=" + shuffleWriteTime +
                '}';
    }
}
