package edu.upenn.cis455.mapreduce.views;

public class SimpleButton {

    private String link;
    private boolean disabled;
    private String content;

    public SimpleButton(String link, boolean disabled, String content) {
        this.link = link; this.disabled = disabled; this.content = content;
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        sb.append("<a class=\"waves-effect waves-light btn-large");
        if (disabled) {
            sb.append(" disabled\"");
        } else {
            sb.append("\"");
        }
        sb.append(" href=\"" + link + "\">");
        sb.append(content);
        sb.append("</a>");
        return sb.toString();
    }

}
