package pt.tecnico.spark.util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Network statistic
 */
@Data
@AllArgsConstructor
public class NetworkStatistic {
    private Integer stageId;
    private String stageName;
    private Double upload;
    private Double download;
}
