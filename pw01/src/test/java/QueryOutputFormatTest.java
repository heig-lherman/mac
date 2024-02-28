import ch.heig.mac.Indices;
import ch.heig.mac.Main;
import ch.heig.mac.Requests;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class QueryOutputFormatTest {

    private Cluster cluster;
    private Requests requests;
    private Indices indices;

    @BeforeEach
    public void setUp() {
        cluster = Main.openConnection();
        requests = new Requests(cluster);
        indices = new Indices(cluster);
    }

    @AfterEach
    void tearDown() {
        cluster.disconnect();
    }

    @Test
    public void testGetMethodNamesOfRequiredIndices() {
        assertThat(indices.getMethodNamesOfRequiredIndices())
                .isSubsetOf(
                    "inconsistentRating",
                    "hiddenGem",
                    "topReviewers",
                    "greatReviewers",
                    "bestMoviesOfActor",
                    "plentifulDirectors",
                    "confusingMovies",
                    "commentsOfDirector1",
                    "commentsOfDirector2",
                    "removeEarlyProjection",
                    "nightMovies"
                );
    }

    @Test
    public void testGetCollectionNamesQuery() {
        assertThat(requests.getCollectionNames())
                .hasSameElementsAs(List.of("comments", "movies", "theaters", "users"));
    }

    @Test
    public void testInconsistentRatingQuery() {
        indices.createRequiredIndicesOf("inconsistentRating");

        JsonObject row = requests.inconsistentRating().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("imdb_id", "tomatoes_rating", "imdb_rating"));
    }

    @Test
    public void testHiddenGemQuery() {
        indices.createRequiredIndicesOf("hiddenGem");

        JsonObject row = requests.hiddenGem().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("title"));
    }

    @Test
    public void testTopReviewersQuery() {
        indices.createRequiredIndicesOf("topReviewers");

        JsonObject row = requests.topReviewers().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("email", "cnt"));
    }

    @Test
    public void testBestMoviesOfActorQuery() {
        indices.createRequiredIndicesOf("bestMoviesOfActor");

        JsonObject row = requests.bestMoviesOfActor("Ralph Fiennes").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("imdb_id", "rating", "cast"));
    }

    @Test
    public void testPlentifulDirectorsQuery() {
        indices.createRequiredIndicesOf("plentifulDirectors");

        JsonObject row = requests.plentifulDirectors().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("director_name", "count_film"));
    }

    @Test
    public void testConfusingMoviesQuery() {
        indices.createRequiredIndicesOf("confusingMovies");

        JsonObject row = requests.confusingMovies().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "title"));
    }

    @Test
    public void testCommentsOfDirector1Query() {
        indices.createRequiredIndicesOf("commentsOfDirector1");

        JsonObject row = requests.commentsOfDirector1("Woody Allen").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "text"));
        assertThatExceptionOfType(ClassCastException.class)
                .isThrownBy(() -> row.getArray("text"));
    }

    @Test
    public void testCommentsOfDirector2Query() {
        indices.createRequiredIndicesOf("commentsOfDirector2");

        JsonObject row = requests.commentsOfDirector2("Woody Allen").get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "text"));
        assertThatExceptionOfType(ClassCastException.class)
                .isThrownBy(() -> row.getArray("text"));
    }

    @Test
    public void testNightMoviesQuery() {
        indices.createRequiredIndicesOf("removeEarlyProjection");

        // ensure at least one movie is only projected late
        requests.removeEarlyProjection("573a13edf29313caabdd49ad");

        JsonObject row = requests.nightMovies().get(0);
        assertThat(row.getNames())
                .hasSameElementsAs(List.of("movie_id", "title"));
    }
}
