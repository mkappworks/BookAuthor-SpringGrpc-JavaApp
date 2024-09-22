package com.mkappworks;

import com.mkappworks.proto.Author;
import com.mkappworks.proto.Book;
import com.mkappworks.proto.BookAuthorServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

import java.util.ArrayList;
import java.util.List;

@GrpcService
public class BookAuthorServerService extends BookAuthorServiceGrpc.BookAuthorServiceImplBase {
    public static List<Author> getAuthorsFromTempDb() {
        return new ArrayList<>() {
            {
                add(Author.newBuilder().setAuthorId(1).setBookId(1).setFirstName("Charles").setLastName("Dickens").setGender("male").build());
                add(Author.newBuilder().setAuthorId(2).setFirstName("William").setLastName("Shakespeare").setGender("male").build());
                add(Author.newBuilder().setAuthorId(3).setFirstName("JK").setLastName("Rowling").setGender("female").build());
                add(Author.newBuilder().setAuthorId(4).setFirstName("Virginia").setLastName("Woolf").setGender("female").build());
            }
        };
    }

    public static List<Book> getBooksFromTempDb() {
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

    @Override
    public void getAuthor(Author request, StreamObserver<Author> responseObserver) {
        getAuthorsFromTempDb()
                .stream()
                .filter(author -> author.getAuthorId() == request.getAuthorId())
                .findFirst()
                .ifPresent(responseObserver::onNext);

        //need to handle error
//        responseObserver.onError();

        responseObserver.onCompleted();
    }
}
