package pt.tecnico.spark.util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Disk statistic
 */
@Data
@AllArgsConstructor
public class DiskStatistic {
    private Integer stageId;
    private String stageName;
    private Double diskRead;
    private Double diskWrite;
}
