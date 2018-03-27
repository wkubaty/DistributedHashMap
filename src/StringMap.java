import java.util.HashMap;
import java.util.Set;

public class StringMap implements SimpleStringMap, java.io.Serializable{
    private HashMap<String, String> hashMap;

    public StringMap(){
        hashMap = new HashMap<>();
    }

    @Override
    public boolean containsKey(String key) {
        return hashMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return hashMap.get(key);
    }

    @Override
    public String put(String key, String value) {
        return hashMap.put(key, value);
    }

    @Override
    public String remove(String key) {
        return hashMap.remove(key);
    }

    public void clear(){
        hashMap.clear();
    }
    public Set<String> keySet(){
        return hashMap.keySet();
    }

}
