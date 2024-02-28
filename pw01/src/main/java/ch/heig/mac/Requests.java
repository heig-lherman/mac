package ch.heig.mac;

import com.couchbase.client.java.query.QueryMetrics;
import com.couchbase.client.java.query.QueryOptions;
import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;


public class Requests {
    private final Cluster ctx;

    public Requests(Cluster cluster) {
        this.ctx = cluster;
    }

    public List<String> getCollectionNames() {
        var result = ctx.query("""
                SELECT RAW r.name
                FROM system:keyspaces r
                WHERE r.`bucket` = "mflix-sample";
                """
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        var result = ctx.query("""
                SELECT imdb.id AS imdb_id,
                       tomatoes.viewer.rating AS tomatoes_rating,
                       imdb.rating AS imdb_rating
                FROM `mflix-sample`._default.movies
                WHERE tomatoes.viewer.rating <> 0
                  AND ABS(tomatoes.viewer.rating - imdb.rating) > 7
                """
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> hiddenGem() {
        var result = ctx.query("""
                SELECT title
                FROM `mflix-sample`._default.movies
                WHERE tomatoes.critic.rating = 10
                AND tomatoes.viewer.rating IS NOT VALUED
                """
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        // Note: we sort by email to ensure a deterministic order in case of a tie in count
        var result = ctx.query("""
                SELECT email, COUNT(email) AS cnt
                FROM `mflix-sample`._default.comments
                GROUP BY email
                ORDER BY cnt DESC, email ASC
                LIMIT 10
                """
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers() {
        var result = ctx.query("""
                SELECT RAW email
                FROM `mflix-sample`._default.comments
                GROUP BY email
                HAVING (COUNT(*) > 300)
                """
        );

        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        var result = ctx.query(
                """
                        SELECT DISTINCT imdb.id AS imdb_id,
                                        imdb.rating,
                                        `cast`
                        FROM `mflix-sample`._default.movies
                        WHERE $actor IN `cast`
                          AND IS_NUMBER(imdb.rating)
                          AND imdb.rating > 8
                        """,
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("actor", actor))
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> plentifulDirectors() {
        var result = ctx.query("""
                SELECT director_name, COUNT(*) AS count_film
                FROM `mflix-sample`._default.movies
                UNNEST directors AS director_name
                GROUP BY director_name
                HAVING COUNT(*) > 30
                """
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> confusingMovies() {
        // Note: according to the schema, the directors array cannot contain null values,
        //       as such we can use ARRAY_LENGTH instead of ARRAY_COUNT.
        var result = ctx.query("""
                SELECT _id AS movie_id, title
                FROM `mflix-sample`._default.movies
                WHERE ARRAY_LENGTH(directors) > 20
                """
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        var result = ctx.query(
                """
                        SELECT m._id AS movie_id,
                               c.text
                        FROM `mflix-sample`._default.movies m
                            JOIN `mflix-sample`._default.comments c ON c.movie_id = m._id
                        WHERE $director IN m.directors
                        """,
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("director", director))
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        var result = ctx.query(
                """
                        SELECT c.movie_id,
                               c.text
                        FROM `mflix-sample`._default.comments c
                        WHERE c.movie_id IN (
                            SELECT RAW m._id
                            FROM `mflix-sample`._default.movies m
                            WHERE $director IN m.directors
                        )
                        """,
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("director", director))
        );

        return result.rowsAs(JsonObject.class);
    }

    // Returns the number of documents updated.
    public long removeEarlyProjection(String movieId) {
        // Filtering on whether the schedule array contains early projections for the given movie
        // to avoid an excessive amount of identity mutations
        // Note: we could use WITHIN for the WHERE clause, but that would add a few more mutations
        // in cases where all schedules are already after 18:00
        var result = ctx.query(
                """
                        UPDATE `mflix-sample`._default.theaters
                        SET schedule = ARRAY s FOR s IN schedule
                        	WHEN s.movieId <> $movieId
                        		OR s.hourBegin >= '18:00:00' END
                        WHERE ANY s IN schedule SATISFIES
                        	s.movieId = $movieId
                        		AND s.hourBegin < '18:00:00' END
                        """,
                QueryOptions
                        .queryOptions()
                        .parameters(JsonObject.create().put("movieId", movieId))
        );

        return result
                .metaData()
                .metrics()
                .map(QueryMetrics::mutationCount)
                .orElse(0L);
    }

    public List<JsonObject> nightMovies() {
        var result = ctx.query("""
                SELECT m._id AS movie_id, m.title
                FROM `mflix-sample`._default.movies m
                WHERE m._id IN (
                    SELECT DISTINCT RAW schedule.movieId
                    FROM `mflix-sample`._default.theaters
                    UNNEST schedule
                    GROUP BY schedule.movieId
                    HAVING MIN(schedule.hourBegin) >= "18:00:00"
                )
                """
        );

        return result.rowsAs(JsonObject.class);
    }


}
