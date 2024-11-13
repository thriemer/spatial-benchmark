package de.thriemer.spatial.evaluation;

public class LatexTableGenerator {

    private static final String longLatexHeader = """
            \\begin{table}[H]
            \\begin{tiny}
            \\begin{longtable}{
            """;

    private static final String longLatexFooter = """
            \\caption{%s}
            \\label{%s}
            \\end{longtable}
            \\end{tiny}
            \\end{table}
            \n
            """;

    public static String generateLongTable(String caption, String label, String[] header, String[][] content, String column) {
        String tableHeader = longLatexHeader + column + "}\n\\hline\n";
        StringBuilder tableBody = new StringBuilder();
        for (int i = 0; i < header.length; i++) {
            tableBody.append(header[i].replace("%", "\\%"));
            if (i != header.length - 1) {
                tableBody.append(" & ");
            }
        }
        tableBody.append("\\\\ \\hline \\endhead\n");
        for (var line : content) {
            for (int i = 0; i < line.length; i++) {
                tableBody.append(line[i].replace("%", "\\%"));
                if (i != line.length - 1) {
                    tableBody.append(" & ");
                }
            }
            tableBody.append("\\\\ \\hline \n");
        }
        return tableHeader + tableBody + longLatexFooter.formatted(caption, label);
    }


    private static final String latexHeader = "\\begin{tabular}{";

    private static final String latexFooter = """
            \\end{tabular}
            """;

    public static String generateTable(String[] header, String[][] content, String column, boolean breakLines) {
        String tableHeader = latexHeader + column + "}\n\\hline\n";
        StringBuilder tableBody = new StringBuilder();
        for (int i = 0; i < header.length; i++) {
            addCell(tableBody, header[i], breakLines);
            if (i != header.length - 1) {
                tableBody.append(" & ");
            }
        }
        tableBody.append("\\\\ \\hline\n");
        for (var line : content) {
            for (int i = 0; i < line.length; i++) {
                addCell(tableBody, line[i], breakLines);
                if (i != line.length - 1) {
                    tableBody.append(" & ");
                }
            }
            tableBody.append("\\\\ \\hline \n");
        }
        return tableHeader + tableBody + latexFooter;
    }

    private static void addCell(StringBuilder sb, String content, boolean breakLines){
        if(breakLines) {
            sb.append("\\makecell{");
        }
        String headerCell = content.replace("%", "\\%");
        if(breakLines) {
            headerCell = headerCell.replace(" ", "\\\\");
        }
        sb.append(headerCell);
        if(breakLines) {
            sb.append("}");
        }
    }

}
