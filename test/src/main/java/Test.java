public class Test {

    public static void main(String[] args) {
        TestA test_a = (int a, int b) -> {return a + b;};
        System.out.println(test_a.operation(1, 2));
    }
}
