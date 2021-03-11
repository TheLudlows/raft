package io.four.raft;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CompletableTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        CompletableFuture<Boolean> f = CompletableFuture.supplyAsync(() -> {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return false;
        })
                .orTimeout(1, TimeUnit.SECONDS)
                .exceptionally(e -> {
                    System.out.println(1);
                    return false;
                });

        System.out.println(f.get());

    }
}
