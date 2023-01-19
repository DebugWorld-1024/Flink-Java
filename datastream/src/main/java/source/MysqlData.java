package source;

public class MysqlData {
    private int id;
    private String exchange_name;
    private String exchange_url;

    public MysqlData(int id, String exchange_name, String exchange_url) {
        this.id = id;
        this.exchange_name = exchange_name;
        this.exchange_url = exchange_url;
    }

    @Override
    public String toString() {
        return "MysqlData{" +
                "id=" + id +
                ", exchange_name='" + exchange_name + '\'' +
                ", exchange_url='" + exchange_url + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getExchange_name() {
        return exchange_name;
    }

    public void setExchange_name(String exchange_name) {
        this.exchange_name = exchange_name;
    }

    public String getExchange_url() {
        return exchange_url;
    }

    public void setExchange_url(String exchange_url) {
        this.exchange_url = exchange_url;
    }
}
