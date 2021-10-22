package cn.ac.iie.mining;

public class MiningModule {
    public static void main(String[] args) {
        MiningService miningService = new MiningService();
        miningService.consumeKafka();
    }
}
