package com.example;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== Project Reactor Demo ===");

        // We'll use CountDownLatch to wait for demos to complete
        CountDownLatch latch = new CountDownLatch(6);

        // DEMO 1: Simple Sequential Processing
        System.out.println("\n=== DEMO 1: Simple Sequential Processing ===");
        Flux.range(1, 5)
                .map(i -> i * i)
                .doOnNext(n -> System.out.println("Square: " + n + " - Thread: " + Thread.currentThread().getName()))
                .doFinally(signal -> latch.countDown())
                .subscribe();

        // DEMO 2: Parallel Processing
        System.out.println("\n=== DEMO 2: Parallel Processing ===");
        CountDownLatch parallelLatch = new CountDownLatch(1);

        Flux.range(1, 10)
                .parallel(4) // Use 4 parallel rails
                .runOn(Schedulers.parallel())
                .map(i -> {
                    System.out.println("Processing: " + i + " - Thread: " + Thread.currentThread().getName());
                    return i * i;
                })
                .sequential() // Convert results back to sequential flow
                .doOnComplete(() -> {
                    System.out.println("All parallel processes completed.");
                    parallelLatch.countDown();
                })
                .doFinally(signal -> latch.countDown())
                .subscribe(n -> System.out.println("Result: " + n));

        // Wait for parallel processes to complete
        try {
            parallelLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // DEMO 3: Difference between flatMap and concatMap
        System.out.println("\n=== DEMO 3: flatMap vs concatMap ===");
        List<String> words = Arrays.asList("Reactor", "Java", "Flux", "Mono", "Reactive");

        System.out.println("*** flatMap (no order guarantee, parallel) ***");
        Flux.fromIterable(words)
                .flatMap(word -> {
                    Random rnd = new Random();
                    return Mono.just(word.toUpperCase())
                            .delayElement(Duration.ofMillis(rnd.nextInt(500))); // random delay
                })
                .doOnNext(word -> System.out.println("flatMap: " + word))
                .doFinally(signal -> latch.countDown())
                .subscribe();

        System.out.println("*** concatMap (order guaranteed, sequential) ***");
        Flux.fromIterable(words)
                .concatMap(word -> {
                    Random rnd = new Random();
                    return Mono.just(word.toUpperCase())
                            .delayElement(Duration.ofMillis(rnd.nextInt(500))); // random delay
                })
                .doOnNext(word -> System.out.println("concatMap: " + word))
                .doFinally(signal -> latch.countDown())
                .subscribe();

        // DEMO 4: Error Handling
        System.out.println("\n=== DEMO 4: Error Handling ===");
        Flux.range(1, 10)
                .map(i -> {
                    if (i == 5) throw new RuntimeException("Error when i = 5!");
                    return i;
                })
                .onErrorReturn(-1) // Return -1 on error
                .doOnNext(i -> System.out.println("onErrorReturn: " + i))
                .doFinally(signal -> latch.countDown())
                .subscribe();

        // DEMO 5: Error Handling - Continue
        System.out.println("\n=== DEMO 5: Error Handling - Continue ===");
        Flux.range(1, 10)
                .flatMap(i -> {
                    if (i == 5) {
                        return Mono.error(new RuntimeException("Error when i = 5!"));
                    }
                    return Mono.just(i);
                })
                .onErrorContinue((error, item) ->
                        System.out.println("Error occurred but we continue. Error: " + error.getMessage())
                )
                .doOnNext(i -> System.out.println("onErrorContinue: " + i))
                .doFinally(signal -> latch.countDown())
                .subscribe();

        // DEMO 6: Useful Operators
        System.out.println("\n=== DEMO 6: Useful Operators ===");
        Flux.range(1, 100)
                .buffer(10) // Collect in groups of 10
                .map(list -> "Group: " + list)
                .take(3) // Take only first 3 groups
                .doOnNext(System.out::println)
                .doFinally(signal -> latch.countDown())
                .subscribe();

        // DEMO 7: Periodic console message every 1 second
        System.out.println("\n=== DEMO 7: Periodic Console Message ===");
        CountDownLatch periodicLatch = new CountDownLatch(1);

        Flux.interval(Duration.ofSeconds(1))
                .take(5) // Run 5 times, then complete
                .doOnNext(i -> System.out.println("Periodic message: " + (i + 1) + ". second"))
                .doOnComplete(() -> {
                    System.out.println("Periodic demo completed.");
                    periodicLatch.countDown();
                })
                .subscribe();

        // Wait for periodic demo to complete
        if (!periodicLatch.await(6, TimeUnit.SECONDS)) {
            System.out.println("Timed out waiting for periodic demo to complete.");
        }
        latch.countDown();

        // Wait for all demos to complete
        System.out.println("\nDemos are running, results coming...");
        if (!latch.await(10, TimeUnit.SECONDS)) {
            System.out.println("Timed out waiting for demos to complete.");
        }
        System.out.println("\n=== All demos completed ===");
    }
}