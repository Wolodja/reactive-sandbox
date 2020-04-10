package com.reactivesandbox;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
public class ReactiveExamplesTest {

    Person michael = new Person("Michael", "Weston");
    Person fiona = new Person("Fiona", "Glenanne");
    Person sam = new Person("Sam", "Axe");
    Person jesse = new Person("Jesse", "Porter");

    @Test
    public void monoTests() {
        //create new person mono
        Mono<Person> personMono = Mono.just(michael);

        //get person object from mono publisher
        Person person = personMono.block();

        // output name
        log.info(person.sayMyName());
    }

    @Test
    public void monoTransform() {
        //create new person mono
        Mono<Person> personMono = Mono.just(fiona);

        //type transformation
        PersonCommand command = personMono
                .map(PersonCommand::new).block();

        log.info(command.sayMyName());
    }

    @Test
    public void monoFilter() {
        Mono<Person> personMono = Mono.just(sam);
        assertThrows(NullPointerException.class,
                () -> {
                    Person samAxe = personMono
                            .filter(person -> person.getFirstName().equalsIgnoreCase("foo"))
                            .block();


                    log.info(samAxe.sayMyName()); //throws NPE
                });
    }

    @Test
    public void fluxTest() {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestFilter() {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.filter(person -> person.getFirstName().equals(fiona.getFirstName()))
                .subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestDelayNoOutput() {

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .subscribe(person -> log.info(person.sayMyName()));

    }

    @Test
    public void fluxTestDelay() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .doOnComplete(countDownLatch::countDown)
                .subscribe(person -> log.info(person.sayMyName()));

        countDownLatch.await();

    }

    @Test
    public void fluxTestFilterDelay() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(1);

        Flux<Person> people = Flux.just(michael, fiona, sam, jesse);

        people.delayElements(Duration.ofSeconds(1))
                .filter(person -> person.getFirstName().contains("i"))
                .doOnComplete(countDownLatch::countDown)
                .subscribe(person -> log.info(person.sayMyName()));

        countDownLatch.await();
    }

    @Test
    public void fluxTestWithNumbers(){
        List<Integer> elements = new ArrayList<>();

        Flux.just(1, 2, 3, 4)
                .log()
                .subscribe(elements::add);

        assertThat(elements).containsExactly(1, 2, 3, 4);
    }

    @Test
    public void fluxTestWithNumberAndFullSubscriber(){
        List<Integer> elements = new ArrayList<>();

        Flux.just(1, 2, 3, 4)
                .log()
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onSubscribe(Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        elements.add(integer);
                    }

                    @Override
                    public void onError(Throwable t) {}

                    @Override
                    public void onComplete() {}
                });
    }

    @Test
    public void fluxTestWithNumbersAndOperations(){
        List<Integer> elements = new ArrayList<>();

        Flux.just(1, 2, 3, 4)
                .log()
                .map(i -> i * 2)
                .subscribe(elements::add);
    }

}