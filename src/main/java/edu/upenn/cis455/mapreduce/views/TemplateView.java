package edu.upenn.cis455.mapreduce.views;

public class TemplateView {

    StringBuilder sb;
    ViewElement rootElement;

    public TemplateView() {
        sb = new StringBuilder();
        appendHeader();
        rootElement = new ViewElement("div", "container", "");
    }

    public void appendHeader() {
        sb.append("<!DOCTYPE html>\n" +
                "<html>\n" +
                "<head>\n" +
                "    <title>Register account</title>\n" +
                "    <!-- Compiled and minified CSS -->\n" +
                "    <link rel=\"stylesheet\" href=\"https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/css/materialize.min.css\">\n" +
                "\n" +
                "    <!-- Compiled and minified JavaScript -->\n" +
                "    <script src=\"https://cdnjs.cloudflare.com/ajax/libs/materialize/1.0.0/js/materialize.min.js\"></script>\n" +
                "</head>\n" +
                "<body>\n");
    }

    public void insertElement(ViewElement element) {
        rootElement.insertElement(element);
    }

    public void appendEnd() {
        sb.append("</body>\n" + "</html>");
    }

    public String render() {
        sb.append(rootElement.render());
        appendEnd();
        return sb.toString();
    }
}
