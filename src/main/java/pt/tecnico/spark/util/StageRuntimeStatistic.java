package pt.tecnico.spark.util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * POJO to save task runtime
 */
@Data
@AllArgsConstructor
public class StageRuntimeStatistic {
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
    private Double cpuUsage;
    private Double systemLoad;
    private Double upload;
    private Double download;
    private Long partialOutputWaitTime;
    private Long initialReadTime;
    private Long memoryInput;
    private Long hadoopInput;
    private Long networkInput;
    private Long diskInput;
}
