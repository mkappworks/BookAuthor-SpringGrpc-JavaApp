package com.mkappworks;

import com.google.protobuf.Descriptors;
import com.mkappworks.proto.Author;
import com.mkappworks.proto.Book;
import com.mkappworks.proto.BookAuthorServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
public class BookAuthorClientService {

    @GrpcClient("grpc-service")
    BookAuthorServiceGrpc.BookAuthorServiceBlockingStub synchronousClient;

    @GrpcClient("grpc-service")
    BookAuthorServiceGrpc.BookAuthorServiceStub asynchronousClient;

    private static List<Author> getAuthorsFromTempDb() {
        return new ArrayList<>() {
            {
                add(Author.newBuilder().setAuthorId(1).setBookId(1).setFirstName("Charles").setLastName("Dickens").setGender("male").build());
                add(Author.newBuilder().setAuthorId(2).setFirstName("William").setLastName("Shakespeare").setGender("male").build());
                add(Author.newBuilder().setAuthorId(3).setFirstName("JK").setLastName("Rowling").setGender("female").build());
                add(Author.newBuilder().setAuthorId(4).setFirstName("Virginia").setLastName("Woolf").setGender("female").build());
            }
        };
    }

    private static List<Book> getBooksFromTempDb() {
        return new ArrayList<>() {
            {
                add(Book.newBuilder().setBookId(1).setAuthorId(1).setTitle("Oliver Twist").setPrice(123.3f).setPages(100).build());
                add(Book.newBuilder().setBookId(2).setAuthorId(1).setTitle("A Christmas Carol").setPrice(223.3f).setPages(150).build());
                add(Book.newBuilder().setBookId(3).setAuthorId(2).setTitle("Hamlet").setPrice(723.3f).setPages(250).build());
                add(Book.newBuilder().setBookId(4).setAuthorId(3).setTitle("Harry Potter").setPrice(423.3f).setPages(350).build());
                add(Book.newBuilder().setBookId(5).setAuthorId(3).setTitle("The Casual Vacancy").setPrice(523.3f).setPages(450).build());
                add(Book.newBuilder().setBookId(6).setAuthorId(4).setTitle("Mrs. Dalloway").setPrice(623.3f).setPages(550).build());
            }
        };
    }

    public Map<Descriptors.FieldDescriptor, Object> getAuthor(int authorId) {
        Author authorRequest = Author.newBuilder().setAuthorId(authorId).build();

        Author authorResponse = synchronousClient.getAuthor(authorRequest);

        return authorResponse.getAllFields();
    }

    public List<Map<Descriptors.FieldDescriptor, Object>> getBooksByAuthor(int authorId) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        Author authorRequest = Author.newBuilder().setAuthorId(authorId).build();
        final List<Map<Descriptors.FieldDescriptor, Object>> response = new ArrayList<>();

        asynchronousClient.getBooksByAuthor(authorRequest, new StreamObserver<>() {
            @Override
            public void onNext(Book book) {
                response.add(book.getAllFields());
            }

            @Override
            public void onError(Throwable throwable) {
                countDownLatch.countDown();
            }

            @Override
            public void onCompleted() {
                countDownLatch.countDown();
            }
        });

        boolean await = countDownLatch.await(1, TimeUnit.MINUTES);
        return await ? response : Collections.emptyList();
    }

    public Map<String, Map<Descriptors.FieldDescriptor, Object>> getExpensiveBook() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final Map<String, Map<Descriptors.FieldDescriptor, Object>> response = new HashMap<>();

        StreamObserver<Book> responseObserver = asynchronousClient.getExpensiveBook(
                new StreamObserver<>() {
                    @Override
                    public void onNext(Book book) {
                        response.put("ExpensiveBook", book.getAllFields());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        countDownLatch.countDown();
                    }
                }
        );

        getBooksFromTempDb()
                .forEach(responseObserver::onNext);

        responseObserver.onCompleted();

        boolean await = countDownLatch.await(1, TimeUnit.MINUTES);
        return await ? response : Collections.emptyMap();
    }

    public List<Map<Descriptors.FieldDescriptor, Object>> getBooksByAuthorGender(String gender) throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        final List<Map<Descriptors.FieldDescriptor, Object>> response = new ArrayList<>();

        StreamObserver<Book> responseObserver = asynchronousClient.getBooksByGender(
                new StreamObserver<>() {
                    @Override
                    public void onNext(Book book) {
                        response.add(book.getAllFields());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        countDownLatch.countDown();
                    }
                }
        );

        getAuthorsFromTempDb()
                .stream()
                .filter(author -> author.getGender().equals(gender))
                .forEach(author -> responseObserver.onNext(
                        Book.newBuilder()
                                .setAuthorId(author.getAuthorId())
                                .build()
                ));

        responseObserver.onCompleted();

        boolean await = countDownLatch.await(1, TimeUnit.MINUTES);
        return await ? response : Collections.emptyList();
    }
}
