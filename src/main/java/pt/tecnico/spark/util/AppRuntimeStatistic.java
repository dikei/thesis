package pt.tecnico.spark.util;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Runtime statistic
 */
@Data
@AllArgsConstructor
public class AppRuntimeStatistic {
    private String name;

    private Double barrier;
    private Double barrierLower;
    private Double barrierUpper;
    private Double barrierStdDev;
    private Long barrierSample;

    private Double noBarrier;
    private Double noBarrierLower;
    private Double noBarrierUpper;
    private Double noBarrierStdDev;
    private Long noBarrierSample;

    private Boolean significantDifferent;

    private Double speedUp;
    private Double speedUpLower;
    private Double speedUpUpper;
}
