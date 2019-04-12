package edu.upenn.cis455.mapreduce.views;

import java.util.ArrayList;
import java.util.List;

public class SimpleTable {

    String[] headers;
    List<Object[]> rows;

    public SimpleTable(String...headers) {
        this.headers = headers;
        this.rows = new ArrayList<>();
    }

    public void addRow(Object...row) {
        rows.add(row);
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append("<table><tr>");
        for (String s : headers) {
            sb.append("<th>" + s + "</th>");
        }
        sb.append("</tr>");
        for (Object[] row : rows) {
            sb.append("<tr>");
            for (Object s : row) {
                sb.append("<td>" + s.toString() + "</td>");
            }
            sb.append("</tr>");
        }
        sb.append("</table>");
        return sb.toString();
    }
}
