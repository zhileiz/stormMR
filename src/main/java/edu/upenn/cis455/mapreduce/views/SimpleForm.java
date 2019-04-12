package edu.upenn.cis455.mapreduce.views;

import java.util.ArrayList;

public class SimpleForm {

    String url;
    String method;
    ArrayList<Field> fields;

    private class Field{
        String name, formName, type, tag;
        public Field(String name, String formName, String type, String tag) {
            this.name = name; this.formName = formName;
            this.tag = tag; this.type = type;
        }
        public String render() {
            StringBuilder sb = new StringBuilder();
            sb.append(name + " <" + tag);
            sb.append(" type=\"" + type + "\"");
            sb.append(" name=\"" + formName + "\"");
            sb.append("<br/>");
            return sb.toString();
        }
    }

    public SimpleForm(String url, String method) {
        this.url = url;
        this.method = method;
        this.fields = new ArrayList<>();
    }

    public void addTextInputField(String name, String valueName) {
        addInputField(name, valueName, "text");
    }

    public void addInputField(String name, String formName, String type) {
        addField(name, formName, type, "input");

    }

    public void addField(String name, String formName, String type, String tag) {
        fields.add(new Field(name, formName, type, tag));
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
//        String onSubmit = "onsubmit=\"this.action = 'create/' + this.name.value;\"";
//        sb.append("<form " + onSubmit + "\" method=\"" + method + "\" target=\"_blank\">");
        sb.append("<form action=\"" + url + "\" method=\"" + method + "\" target=\"_blank\">");
        for (Field field : fields) {
            sb.append(field.render());
        }
        sb.append("<button class=\"btn waves-effect waves-light\" type=\"submit\" name=\"action\">Submit\n</button>");
        sb.append("</form>");
        return sb.toString();
    }
}
