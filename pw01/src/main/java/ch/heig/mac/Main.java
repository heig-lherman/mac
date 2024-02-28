package ch.heig.mac;

import com.couchbase.client.java.Cluster;

public class Main {

    public static Cluster openConnection() {
        var connectionString = "127.0.0.1";
        var username = "Administrator";
        var password = "mac2024";

        Cluster cluster = Cluster.connect(
                connectionString,
                username,
                password
        );
        return cluster;
    }

    public static void run(Cluster ctx) {

        var requests = new Requests(ctx);
        var indices = new Indices(ctx);

        indices.createRequiredIndices();

        requests.getCollectionNames().forEach(System.out::println);
    }

    public static void main(String[] args) {
        try (var cluster = openConnection()) {
            run(cluster);
        }
    }
}
