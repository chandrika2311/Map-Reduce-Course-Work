package wiki;


import org.xml.sax.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.awt.font.TextLayout;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class parsing{

    private String value;

    public parsing(String val){
        this.value = val;
    }
    public String func1() throws SAXException, ParserConfigurationException {
        Pattern namePattern;
        Pattern ampercent;
        Pattern linkPattern;
        String links="";
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");


        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        SAXParser saxParser = spf.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        // Parser fills this list with linked page names.
        Set<String> linkPageNames = new HashSet<String>();
        xmlReader.setContentHandler(new WikiParser(linkPageNames));
        String line;
        // Each line formatted as (Wiki-page-name:Wiki-page-html).

        int delimLoc = this.value.toString().indexOf(':');
        String pageName = this.value.toString().substring(0, delimLoc);
        String html = this.value.toString().substring(delimLoc + 1);
        html = html.replace("&", "&amp;");
        Matcher matcher = namePattern.matcher(pageName);
        if (matcher.find()) {
            // Skip this html file, name contains (~).
            // Parse page and fill list of linked pages.
            linkPageNames.clear();
            try {
                xmlReader.parse(new InputSource(new StringReader(html)));
                links = String.join(",", linkPageNames);
//                String[] linkList =  links.split(",");

            }catch (Exception e){
                e.printStackTrace();}
        }

        return  pageName +':'+links;
    }
}