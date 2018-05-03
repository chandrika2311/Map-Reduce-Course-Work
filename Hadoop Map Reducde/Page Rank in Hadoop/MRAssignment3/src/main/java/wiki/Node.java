package wiki;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node {
    private String nodeId;
    private ArrayList<String> adjecencyList;
    private double pageRank;
    public Node(){

    }

    public String getNodeId(){
        return this.nodeId;
    }
    public ArrayList<String> getAdjecencylist(){
        return this.adjecencyList;
    }
    public double getPageRank(){
        return this.pageRank;
    }
    public void setNodeId(String nodeId){
        this.nodeId = nodeId;

    }
    public void setAdjecencylist(String adjecencyList){
        if (adjecencyList.length() == 0){
            this.adjecencyList = new ArrayList<String>();
        }else{
            adjecencyList = adjecencyList.replace("[","");
            adjecencyList = adjecencyList.replace("[","");
            String[] adjecencyList_values = adjecencyList.split(",");
            this.adjecencyList = new ArrayList<String>(Arrays.asList(adjecencyList_values));
        }
    }
    public void setPageRank(double pageRank, double oldDeltaCounterValue, double numVertices){
        double alpha = 0.15;
        this.pageRank = pageRank+ (1-alpha)* (oldDeltaCounterValue/(double)numVertices);
    }
    public void setPageRank(double pageRank){

        this.pageRank=pageRank;
    }
    public void appendLinktoAdjecencyList(String link){
        this.adjecencyList.add(link);
    }
    public void removeLinkFromAdjecencyList(String link){

        this.adjecencyList.remove(link);
    }
}
