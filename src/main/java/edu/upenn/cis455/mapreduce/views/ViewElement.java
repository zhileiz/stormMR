package edu.upenn.cis455.mapreduce.views;

import java.util.ArrayList;

public class ViewElement {


    private String content;
    private String tag;
    private String className;
    private ArrayList<ViewElement> innerElements;

    boolean stringOnly;

    public ViewElement(String str) {
        this.content = str;
        stringOnly = true;
    }

    public ViewElement(String tag, String className, String content) {
        this.tag = tag;
        this.stringOnly = false;
        this.className = className;
        this.innerElements = new ArrayList<>();
        innerElements.add(new ViewElement(content));
    }

    public ViewElement(String tag, String className, ViewElement... elements) {
        this.tag = tag;
        this.stringOnly = false;
        this.className = className;
        this.innerElements = new ArrayList<>();
        for (ViewElement element : elements) {
            innerElements.add(element);
        }
    }

    public void insertElement(ViewElement element) {
        this.innerElements.add(element);
    }


    public String render() {
        if (stringOnly) { return this.content; }
        StringBuilder sb = new StringBuilder();
        sb.append("<" + tag);
        if (className != null) {
            sb.append(" class=" + className);
        }
        sb.append(">");
        for (ViewElement e : innerElements) {
            sb.append(e.render());
        }
        sb.append("</" + tag + ">\n");
        return sb.toString();
    }
}
