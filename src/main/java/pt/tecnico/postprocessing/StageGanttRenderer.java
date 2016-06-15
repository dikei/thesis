package pt.tecnico.postprocessing;

import org.jfree.chart.renderer.category.GanttRenderer;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeriesCollection;

import java.awt.*;
import java.util.List;

class StageGanttRenderer extends GanttRenderer {

    private final TaskSeriesCollection collection;
    private int row;
    private int col;
    private int index;
    private Color[] pallete = new Color[] {
            new Color(228,26,28),
            new Color(55,126,184),
            new Color(77,175,74),
            new Color(152,78,163)
    };

    public StageGanttRenderer(TaskSeriesCollection collection) {
        this.collection = collection;
    }

    @Override
    public Paint getItemPaint(int row, int col) {
        if (this.row != row || this.col != col) {
            this.row = row;
            this.col = col;
            index = 0;
        }
        if (index > 0) {
            // Sub task will be draw with index > 0
            String series = (String) collection.getSeriesKey(row);
            if (col < collection.getSeries(series).getItemCount()) {
                Task t = (Task) collection.getSeries(series).getTasks().get(col);
                if (index - 1 < t.getSubtaskCount()) {
                    String subTaskName = t.getSubtask(index - 1).getDescription();
                    if (subTaskName.startsWith("Wait")) {
                        index++;
                        return new Color(64, 64, 64, 128);
                    }
                } else {
                    System.out.println("Unexpected index: " + index);
                }
            } else {
                System.out.printf("Unexpected row: %s col: %d %n", series, col);
                List<Task> tasks = collection.getSeries(series).getTasks();
                for(Task t: tasks) {
                    System.out.println(t.getDescription());
                }
                index++;
                return Color.LIGHT_GRAY;
            }
        }
        index++;
        return super.getItemPaint(row, col);
    }
}