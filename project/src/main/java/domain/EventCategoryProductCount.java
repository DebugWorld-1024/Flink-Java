package domain;

public class EventCategoryProductCount {

    public String event;
    public String category;
    public String product;
    public long count;
    public long start;
    public long end;

    public EventCategoryProductCount() {
    }

    public EventCategoryProductCount(String event, String category, String product, long count, long start, long end) {
        this.event = event;
        this.category = category;
        this.product = product;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    @Override
    public String toString() {
       return event + "\t" + category + "\t" + product + "\t" + count + "\t" + start + "\t" + end;
    }
}
