package edu.upenn.cis455.mapreduce.master.routes;

import edu.upenn.cis455.mapreduce.master.MasterConfig;
import edu.upenn.cis455.mapreduce.master.WorkerRecord;
import edu.upenn.cis455.mapreduce.views.*;
import spark.Request;
import spark.Response;
import spark.Route;

import java.util.List;

public class StatusRoute implements Route {

    private MasterConfig master;

    public StatusRoute(MasterConfig master) {
        this.master = master;
    }

    @Override
    public Object handle(Request req, Response res) {
        TemplateView view = new TemplateView();
        addTitleToView("h3", view, "Worker Status");
        addTableToView(view);
        addTitleToView("h3", view, "Submit Job");
        addFormToView(view);
        return view.render();
    }

    private void addTableToView(TemplateView view) {
        SimpleTable tb = new SimpleTable("ip:port", "status", "keysRead", "keysWritten");
        List<WorkerRecord> records = master.getWorkerRecords();
        for (WorkerRecord rec : records) {
            tb.addRow(rec.getIp(), rec.getStatus(), rec.getKeysRead(), rec.getKeysWritten());
        }
        view.insertElement(new ViewElement(tb.render()));
    }

    private void addTitleToView(String tag, TemplateView view, String title) {
        view.insertElement(new ViewElement(tag, null, title));
    }

    private void addFormToView(TemplateView view) {
        SimpleForm form = new SimpleForm("/job", "post");
        form.addTextInputField("Class Name", "class_name");
        form.addTextInputField("Input Directory", "input_directory_path");
        form.addTextInputField("Output Directory", "output_directory_path");
        form.addInputField("Number of MapBolts", "num_map_bolts", "number");
        form.addInputField("Number of ReduceBolts", "num_reduce_bolts", "number");
        view.insertElement(new ViewElement(form.render()));
    }
}
