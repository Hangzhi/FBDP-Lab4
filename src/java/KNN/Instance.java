
public class Instance {
    private double[] attributeValue;
    private double lable;
    private String id;

    /**
     * a line of form a1 a2 ...an lable
     * @param line
     */
    public Instance(String line){
        String[] value = line.split(" ");
        attributeValue = new double[value.length - 1];
        for(int i = 1;i < attributeValue.length;i++){
            attributeValue[i] = Double.parseDouble(value[i]);
        }
        lable = Double.parseDouble(value[value.length - 1]);
    }

    public double[] getAtrributeValue(){
        return attributeValue;
    }

    public double getLabel(){
        return lable;
    }

    public String getId(){
        return id;
    }

    public void setId(String Id){
        this.id=Id;
    }
}
